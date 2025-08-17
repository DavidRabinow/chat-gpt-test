import os, io, zipfile, shutil, tempfile, yaml, re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from loguru import logger
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter
from rapidfuzz import fuzz, process

ROOT = Path(__file__).resolve().parent
PATTERNS = yaml.safe_load(open(ROOT / 'config' / 'patterns.yaml', 'r', encoding='utf-8'))
MAPPING = yaml.safe_load(open(ROOT / 'config' / 'mapping.yaml', 'r', encoding='utf-8'))

# Enhanced field type classification with fuzzy matching support
FIELD_MAP = {
    "name": ["name", "names", "name(s)", "full name", "legal name", "business name", "company name"],
    "email": ["email", "email address", "e-mail", "e-mail address"],
    "address": ["address", "street address", "mailing address", "business address", "current address", "physical address"],
    "phone": ["phone", "telephone", "phone number", "telephone number", "mobile", "cell", "daytime phone"],
    "ein": ["ein", "employer identification number", "tax id", "tax identification number"],
    "dob": ["dob", "date of birth", "birthdate", "birth date"],
    "ssn": ["ssn", "social security", "social security number", "federal tax identification number"]
}

# Field validation patterns
FIELD_VALIDATION = {
    "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    "phone": r'^[\d\s\-\(\)\.]+$',
    "ssn": r'^\d{3}-?\d{2}-?\d{4}$',
    "ein": r'^\d{2}-?\d{7}$',
    "dob": r'^\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}$'
}

# Confidence thresholds
MIN_CONFIDENCE = 80  # Minimum fuzzy match confidence
MIN_FIELD_CONFIDENCE = 70  # Minimum confidence for field detection
MIN_BLANK_SPACE_CONFIDENCE = 60  # Minimum confidence for blank space detection

def process_zip(zip_bytes: bytes, values: Dict[str, str]) -> bytes:
    logger.info(f"Processing ZIP with values: {list(values.keys())}")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        in_dir = tmp_path / 'in'
        out_dir = tmp_path / 'out'
        in_dir.mkdir(); out_dir.mkdir()

        zpath = in_dir / 'input.zip'
        with open(zpath, 'wb') as fz:
            fz.write(zip_bytes)

        pdfs = []
        with zipfile.ZipFile(zpath, 'r') as zf:
            for name in zf.namelist():
                if name.lower().endswith('.pdf'):
                    target = in_dir / Path(name).name
                    with zf.open(name) as src, open(target, 'wb') as dst:
                        dst.write(src.read())
                    pdfs.append(target)

        for pdf in pdfs:
            logger.info(f"Processing PDF: {pdf.name}")
            out_pdf = out_dir / f"filled_{pdf.name}"
            ok = fill_pdf(pdf, out_pdf, values)
            if not ok:
                logger.warning(f"No fields filled in {pdf.name}, copying original")
                import shutil
                shutil.copy2(pdf, out_dir / f"original_{pdf.name}")

        mem = io.BytesIO()
        with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as zfo:
            for p in out_dir.iterdir():
                zfo.write(p, arcname=p.name)
        mem.seek(0)
        return mem.read()

def fill_pdf(src_path: Path, dst_path: Path, values: Dict[str, str]) -> bool:
    logger.info(f"Filling PDF: {src_path.name}")
    
    # Validate input values
    validated_values = validate_input_values(values)
    
    # Try AcroForm first
    if detect_acroform_fields(src_path):
        logger.info("AcroForm fields detected, attempting to fill")
        aliases = {f['key']: f.get('acroform_names', []) for f in MAPPING['fields']}
        ok = fill_acroform(src_path, dst_path, validated_values, aliases)
        if ok:
            logger.info("Successfully filled AcroForm fields")
            return True
    
    # Fall back to text-based field detection
    logger.info("No AcroForm fields found, using text-based detection")
    anchors = search_labels_positions_enhanced(src_path, validated_values)
    ok2 = overlay_values_enhanced(src_path, dst_path, anchors, validated_values, MAPPING)
    return ok2

def validate_input_values(values: Dict[str, str]) -> Dict[str, str]:
    """
    Validate input values against expected patterns and return only valid ones.
    """
    validated = {}
    
    for field_type, value in values.items():
        if not value or not value.strip():
            continue
            
        value = value.strip()
        
        # Check if field type has validation pattern
        if field_type in FIELD_VALIDATION:
            pattern = FIELD_VALIDATION[field_type]
            if not re.match(pattern, value, re.IGNORECASE):
                logger.warning(f"Invalid {field_type} format: '{value}' - skipping")
                continue
        
        validated[field_type] = value
        logger.info(f"Validated {field_type}: '{value}'")
    
    return validated

def detect_acroform_fields(pdf_path: Path):
    try:
        reader = PdfReader(str(pdf_path))
        return reader.get_fields() or {}
    except Exception as e:
        logger.debug(f'AcroForm detection error: {e}')
        return {}

def fill_acroform(pdf_path: Path, out_path: Path, values: Dict[str,str], field_aliases: Dict[str, List[str]]) -> bool:
    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    fields = writer.get_fields() or {}
    update_map = {}
    
    logger.info(f"Available AcroForm fields: {list(fields.keys())}")
    
    for logical_key, aliases in field_aliases.items():
        val = values.get(logical_key)
        if not val:
            continue
        for name in (fields.keys() if fields else []):
            if name in aliases:
                update_map[name] = val
                logger.info(f"AcroForm: Filling '{name}' with '{logical_key}' value")
    
    if not update_map:
        logger.warning("No AcroForm fields matched")
        return False
    
    writer.update_page_form_field_values(writer.pages[0], update_map)
    for j in range(len(writer.pages)):
        page = writer.pages[j]
        if '/Annots' in page:
            del page['/Annots']
    with open(out_path, 'wb') as fw:
        writer.write(fw)
    return True

def classify_field_type(label_text: str) -> Tuple[str, float]:
    """
    Classify a label text to determine its field type using fuzzy matching.
    Returns (field_type, confidence_score)
    """
    label_lower = label_text.lower().strip()
    
    # Remove common punctuation and normalize
    label_clean = label_lower.replace(':', '').replace('.', '').strip()
    
    best_match = None
    best_score = 0
    
    # Priority scoring for more specific matches
    for field_type, keywords in FIELD_MAP.items():
        for keyword in keywords:
            # Try exact match first
            if label_clean == keyword:
                return field_type, 100.0
            
                        # Check for substring matches (more specific)
            if keyword in label_clean or label_clean in keyword:
                # Special case: if both "email" and "address" appear, prioritize email
                if "email" in label_clean and "address" in label_clean:
                    logger.info(f"Field classification: '{label_text}' contains both 'email' and 'address', prioritizing email")
                    return "email", 95.0
                
                # Give higher priority to more specific matches
                if field_type == "email" and "email" in label_clean:
                    return field_type, 95.0
                elif field_type == "address" and "address" in label_clean and "email" not in label_clean:
                    return field_type, 90.0
                elif field_type == "phone" and any(phone_word in label_clean for phone_word in ["phone", "telephone", "mobile", "cell"]):
                    return field_type, 90.0
                elif field_type == "ssn" and any(ssn_word in label_clean for ssn_word in ["ssn", "social security"]):
                    return field_type, 90.0
                elif field_type == "ein" and any(ein_word in label_clean for ein_word in ["ein", "employer identification", "tax id"]):
                    return field_type, 90.0
                elif field_type == "dob" and any(dob_word in label_clean for dob_word in ["dob", "date of birth", "birth"]):
                    return field_type, 90.0
                elif field_type == "name" and "name" in label_clean:
                    return field_type, 85.0
            
            # Try fuzzy matching as fallback
            score = fuzz.ratio(label_clean, keyword)
            if score > best_score:
                best_score = score
                best_match = field_type
    
    return best_match, best_score

def is_likely_field_label(word_info: Tuple, page_width: float, page_height: float) -> bool:
    """
    Determine if a word is likely a field label based on position and context.
    """
    x0, y0, x1, y1, text, *_ = word_info
    
    # Check if text ends with common field indicators
    text_lower = text.lower().strip()
    field_indicators = [':', '.', '?']
    has_field_indicator = any(text_lower.endswith(indicator) for indicator in field_indicators)
    
    # Check if text contains field-related keywords
    field_keywords = ['name', 'email', 'address', 'phone', 'telephone', 'dob', 'birth', 'ssn', 'ein', 'fein', 'daytime']
    has_field_keywords = any(keyword in text_lower for keyword in field_keywords)
    
    # Check position - field labels are often in top-left areas, but can be anywhere
    is_top_left = y0 < page_height * 0.3  # Top 30% of page
    
    # Check if text is relatively short (typical for labels)
    is_short_text = len(text.strip()) < 50  # Increased limit for compound labels
    
    # Check if text is isolated (not part of a paragraph)
    word_width = x1 - x0
    word_height = y1 - y0
    is_isolated = word_width < page_width * 0.3  # Increased limit for longer labels
    
    # Calculate confidence score
    confidence = 0
    if has_field_indicator:
        confidence += 30
    if has_field_keywords:
        confidence += 40  # Higher weight for field keywords
    if is_top_left:
        confidence += 15
    if is_short_text:
        confidence += 20
    if is_isolated:
        confidence += 20
    
    return confidence >= MIN_FIELD_CONFIDENCE

def detect_blank_space_after_label(page, label_bbox: List[float], page_width: float) -> Tuple[bool, List[float]]:
    """
    Detect if there's blank space after a label where we can place text.
    Returns (is_blank, placement_bbox)
    """
    label_x0, label_y0, label_x1, label_y1 = label_bbox
    
    # Define search area after the label with more precise boundaries
    search_x0 = label_x1 + 5  # Start 5 points after label
    search_x1 = min(label_x1 + 150, page_width)  # Search up to 150 points for more precise placement
    search_y0 = label_y0 - 2  # Slightly above label
    search_y1 = label_y1 + 2  # Slightly below label
    
    # Get text in the search area
    search_rect = fitz.Rect(search_x0, search_y0, search_x1, search_y1)
    text_in_area = page.get_text("text", clip=search_rect).strip()
    
    # Check if area is mostly blank or contains placeholder text
    # Also check for common placeholder patterns that we should replace
    placeholder_patterns = ['( )', '___', '...', '/ /', 'if different than above', 'Phone Number:', 'Address:', 'Date of Birth:', 'SSN/FEIN:']
    has_placeholder = any(pattern in text_in_area for pattern in placeholder_patterns)
    
    # Check for existing content that should not be replaced
    # Only check for actual email addresses (with @) and phone numbers
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    phone_patterns = ['\\(\\d{3}\\) \\d{3}-\\d{4}', '\\d{3}-\\d{3}-\\d{4}', '\\d{10}', '\\d{11}']
    
    has_existing_email = re.search(email_pattern, text_in_area)
    has_existing_phone = any(re.search(pattern, text_in_area) for pattern in phone_patterns)
    
    # Consider it blank if it has very little text or only placeholders
    # But don't fill if there's already an email or phone number
    # Be more lenient with text length for better field detection
    is_blank = (len(text_in_area) < 60 or has_placeholder) and not has_existing_email and not has_existing_phone
    
    if is_blank:
        # Calculate placement position with better field boundaries
        placement_x = label_x1 + 30  # 30 points after label for better spacing
        # Create a larger field area for better input positioning
        field_height = max(label_y1 - label_y0, 20)  # Taller field height for better spacing
        placement_y = label_y0 - 4  # Start further above label baseline
        placement_width = search_x1 - placement_x
        placement_height = field_height + 8  # More padding for better alignment
        
        placement_bbox = [placement_x, placement_y, placement_x + placement_width, placement_y + placement_height]
        logger.debug(f"Blank space detected after label at {label_bbox}, placement at {placement_bbox}")
        return True, placement_bbox
    else:
        logger.debug(f"No blank space detected after label at {label_bbox}, found text: '{text_in_area[:50]}...'")
        return False, []

def search_labels_positions_enhanced(pdf_path: Path, values: Dict[str, str]) -> Dict[str, List]:
    """
    Enhanced label search with field type classification, confidence scoring, and blank space detection.
    """
    doc = fitz.open(str(pdf_path))
    hits = {k: [] for k in FIELD_MAP.keys()}
    
    logger.info(f"Searching for field labels in {pdf_path.name}")
    
    for p in range(len(doc)):
        page = doc[p]
        page_width = page.rect.width
        page_height = page.rect.height
        words = page.get_text('words')
        
        logger.info(f"Page {p+1}: Analyzing {len(words)} text elements")
        
        for word_info in words:
            x0, y0, x1, y1, text, *_ = word_info
            
            # Log all potential field labels for debugging
            if any(keyword in text.lower() for keyword in ['email', 'phone', 'address', 'name', 'dob', 'ssn', 'ein']):
                logger.debug(f"Potential field label found: '{text}' at position ({x0:.1f}, {y0:.1f})")
            
            # Skip if not likely a field label
            if not is_likely_field_label(word_info, page_width, page_height):
                continue
            
            # Classify the field type
            field_type, confidence = classify_field_type(text)
            
            if field_type and confidence >= MIN_CONFIDENCE:
                # Check if we have a value for this field type
                if field_type in values and values[field_type]:
                    # Check for blank space after the label
                    is_blank, placement_bbox = detect_blank_space_after_label(page, [x0, y0, x1, y1], page_width)
                    
                    # Additional check: don't fill if there's already substantial content
                    if is_blank and placement_bbox:
                        # Check the placement area for existing content
                        check_rect = fitz.Rect(placement_bbox[0], placement_bbox[1], placement_bbox[2], placement_bbox[3])
                        existing_text = page.get_text("text", clip=check_rect).strip()
                        
                        # Skip if there's already an email or phone number
                        # But allow filling if there's just text like "if different than above"
                        phone_patterns = ['\\(\\d{3}\\) \\d{3}-\\d{4}', '\\d{3}-\\d{3}-\\d{4}', '\\d{10}', '\\d{11}']
                        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                        
                        has_phone = any(re.search(pattern, existing_text) for pattern in phone_patterns)
                        has_email = re.search(email_pattern, existing_text)
                        
                        if has_phone or has_email:
                            logger.debug(f"Skipping field '{text}' - already has email/phone: '{existing_text[:30]}...'")
                            continue
                    
                    if is_blank:
                        hits[field_type].append({
                            'page': p, 
                            'label_bbox': [x0, y0, x1, y1],
                            'placement_bbox': placement_bbox,
                            'text': text,
                            'confidence': confidence
                        })
                        logger.info(f"Found field label: '{text}' → {field_type} (confidence: {confidence:.1f}%) with blank space")
                    else:
                        logger.debug(f"Found field label '{text}' → {field_type} but no blank space available")
                else:
                    logger.debug(f"Found field label '{text}' → {field_type} but no value provided")
            else:
                logger.debug(f"Low confidence match: '{text}' → {field_type} (confidence: {confidence:.1f}%)")
    
    doc.close()
    
    # Log summary
    for field_type, matches in hits.items():
        if matches:
            logger.info(f"Field type '{field_type}': {len(matches)} matches found")
    
    return hits

def overlay_values_enhanced(pdf_path: Path, out_path: Path, anchors: Dict, values: Dict[str, str], mapping: Dict) -> bool:
    """
    Enhanced value overlay with better positioning, validation, and formatting.
    """
    doc = fitz.open(str(pdf_path))
    wrote = False
    
    logger.info(f"Overlaying values for {len(anchors)} field types")
    
    for field_type, matches in anchors.items():
        if not matches:
            continue
            
        val = values.get(field_type)
        if not val:
            logger.debug(f"No value provided for field type: {field_type}")
            continue
        
        # Use the highest confidence match
        best_match = max(matches, key=lambda x: x.get('confidence', 0))
        
        logger.info(f"Filling '{field_type}' with value '{val}' at position {best_match['placement_bbox']}")
        
        # Get positioning from mapping or use defaults
        entry = next((f for f in mapping.get('fields', []) if f['key'] == field_type), None)
        if entry:
            dx = entry['write'].get('offset', {}).get('dx', 10)
            dy = entry['write'].get('offset', {}).get('dy', 0)
            size = entry['write'].get('font_size', 10)  # Slightly smaller font for better fit
        else:
            dx, dy, size = 10, 0, 10  # Default to smaller font
        
        page = doc[best_match['page']]
        placement_bbox = best_match['placement_bbox']
        
        # Calculate text position within the placement area with field-specific alignment
        x = placement_bbox[0] + dx + 8  # Add extra 8 points to move text further right
        
        # Field-specific positioning adjustments for better alignment
        if field_type == "phone":
            # Phone numbers should align with the baseline of the label
            y = placement_bbox[1] + (placement_bbox[3] - placement_bbox[1]) * 0.15 + dy
        elif field_type == "name":
            # Names should be positioned after "above:" if that text is present
            check_rect = fitz.Rect(placement_bbox[0], placement_bbox[1], placement_bbox[2], placement_bbox[3])
            existing_text = page.get_text("text", clip=check_rect).strip()
            if "above:" in existing_text:
                # If "above:" is present, position the name much lower to avoid overlap
                y = placement_bbox[1] + (placement_bbox[3] - placement_bbox[1]) * 0.95 + dy
            else:
                # If no "above:" text, position normally
                y = placement_bbox[1] + (placement_bbox[3] - placement_bbox[1]) * 0.25 + dy
        elif field_type == "email":
            # Email addresses should align with the label baseline
            y = placement_bbox[1] + (placement_bbox[3] - placement_bbox[1]) * 0.25 + dy
        elif field_type == "address":
            # Addresses should align with the label baseline
            y = placement_bbox[1] + (placement_bbox[3] - placement_bbox[1]) * 0.25 + dy
        else:
            # Default positioning in the middle
            field_height = placement_bbox[3] - placement_bbox[1]
            y = placement_bbox[1] + (field_height * 0.25) + dy
        
        # Format text based on field type
        formatted_val = format_field_value(field_type, val)
        
        # Insert text with proper formatting
        page.insert_text((x, y), formatted_val, fontname='helv', fontsize=size)
        wrote = True
        
        logger.info(f"Successfully inserted '{formatted_val}' for field '{field_type}'")
    
    if wrote:
        doc.save(str(out_path))
        logger.info(f"PDF saved with {len([k for k, v in anchors.items() if v])} filled fields")
    else:
        logger.warning("No fields were filled")
    
    doc.close()
    return wrote

def format_field_value(field_type: str, value: str) -> str:
    """
    Format field values based on their type for better presentation.
    """
    if field_type == "phone":
        # Clean and format phone number
        cleaned = re.sub(r'[^\d]', '', value)
        if len(cleaned) == 10:
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        elif len(cleaned) == 11 and cleaned[0] == '1':
            return f"({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:]}"
        return value
    
    elif field_type == "ssn":
        # Format SSN with dashes
        cleaned = re.sub(r'[^\d]', '', value)
        if len(cleaned) == 9:
            return f"{cleaned[:3]}-{cleaned[3:5]}-{cleaned[5:]}"
        return value
    
    elif field_type == "ein":
        # Format EIN with dash
        cleaned = re.sub(r'[^\d]', '', value)
        if len(cleaned) == 9:
            return f"{cleaned[:2]}-{cleaned[2:]}"
        return value
    
    elif field_type == "address":
        # Ensure address is properly formatted
        return value.strip()
    
    return value

# Keep the original functions for backward compatibility
def search_labels_positions(pdf_path: Path, label_patterns):
    doc = fitz.open(str(pdf_path))
    hits = {k: [] for k in label_patterns.keys()}
    for p in range(len(doc)):
        page = doc[p]
        words = page.get_text('words')
        for label_key, variants in label_patterns.items():
            for var in variants:
                v = var.lower()
                for w in words:
                    if v in w[4].lower():
                        x0,y0,x1,y1,_text,*_ = w
                        hits[label_key].append({'page': p, 'bbox': [x0,y0,x1,y1]})
                        break
    doc.close()
    return hits

def overlay_values(pdf_path: Path, out_path: Path, anchors, values, mapping) -> bool:
    doc = fitz.open(str(pdf_path))
    wrote = False
    for entry in mapping.get('fields', []):
        key = entry['key']
        val = values.get(key)
        if not val:
            continue
        anchor_label = entry['write']['anchor_label']
        cands = anchors.get(anchor_label, [])
        if not cands:
            continue
        chosen = cands[0]
        dx = entry['write'].get('offset', {}).get('dx', 10)
        dy = entry['write'].get('offset', {}).get('dy', 0)
        size = entry['write'].get('font_size', 11)
        page = doc[chosen['page']]
        x = chosen['bbox'][2] + dx
        y = chosen['bbox'][1] + dy + (chosen['bbox'][3]-chosen['bbox'][1])*0.8
        page.insert_text((x, y), str(val), fontname='helv', fontsize=size)
        wrote = True
    if wrote:
        doc.save(str(out_path))
    doc.close()
    return wrote
