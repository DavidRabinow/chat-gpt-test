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

# Patterns to detect if fields are already filled
FIELD_DETECTION_PATTERNS = {
    "email": r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # Contains @ symbol and valid email format
    "phone": r'[\d\s\-\(\)\.]{7,}',  # Contains at least 7 digits/phone characters
    "ssn": r'\d{2,3}-?\d{2}-?\d{4}',  # SSN pattern (2-3 digits, 2 digits, 4 digits)
    "ein": r'\d{2}-?\d{7}',  # EIN pattern
    "dob": r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}',  # Date pattern
    "name": r'[A-Za-z]{2,}',  # At least 2 letters
    "address": r'[A-Za-z0-9\s,\.]{10,}'  # At least 10 characters with letters/numbers
}

# Enhanced phone number detection patterns
PHONE_DETECTION_PATTERNS = [
    r'\(\d{3}\)\s*\d{3}-\d{4}',  # (123) 456-7890
    r'\d{3}-\d{3}-\d{4}',        # 123-456-7890
    r'\d{3}\.\d{3}\.\d{4}',      # 123.456.7890
    r'\d{10}',                   # 1234567890
    r'\d{3}\s\d{3}\s\d{4}',      # 123 456 7890
    r'\(\d{3}\)\s\d{3}\s\d{4}',  # (123) 456 7890
]

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
                # Check if the field already has a value
                current_value = fields[name].get('/V', '') if fields[name] else ''
                if current_value:
                    # Check if the current value appears to be valid data
                    if is_acroform_field_already_filled(logical_key, current_value):
                        logger.info(f"AcroForm field '{name}' already contains valid data: '{current_value}', skipping")
                        continue
                
                update_map[name] = val
                logger.info(f"AcroForm: Filling '{name}' with '{logical_key}' value")
    
    if not update_map:
        logger.warning("No AcroForm fields matched or all fields already filled")
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

def detect_blank_space_after_label(page, label_bbox: List[float], page_width: float, field_type: str = None) -> Tuple[bool, List[float]]:
    """
    Detect if there's blank space after a label where we can place text.
    Returns (is_blank, placement_bbox)
    """
    label_x0, label_y0, label_x1, label_y1 = label_bbox
    
    # For phone numbers, try to place them closer to the label
    if field_type == "phone":
        search_positions = [
            (label_x1 + 5, 80),    # 5 points right, 80 points wide - very close
            (label_x1 + 10, 100),  # 10 points right, 100 points wide
            (label_x1 + 15, 120),  # 15 points right, 120 points wide
            (label_x1 + 20, 150),  # 20 points right, 150 points wide
            (label_x1 + 30, 200),  # 30 points right, 200 points wide
        ]
    else:
        # For other fields, use the original positioning
        search_positions = [
            (label_x1 + 50, 50),   # 50 points right, 50 points wide
            (label_x1 + 100, 100), # 100 points right, 100 points wide
            (label_x1 + 150, 150), # 150 points right, 150 points wide
            (label_x1 + 200, 200), # 200 points right, 200 points wide
            (label_x1 + 250, 250), # 250 points right, 250 points wide
        ]
    
    for start_x, width in search_positions:
        search_x0 = start_x
        search_x1 = min(start_x + width, page_width)
        search_y0 = label_y0 - 8  # Slightly above label
        search_y1 = label_y1 + 8  # Slightly below label
        
        # Get text in the search area
        search_rect = fitz.Rect(search_x0, search_y0, search_x1, search_y1)
        text_in_area = page.get_text("text", clip=search_rect).strip()
        
        # For phone fields, be more lenient with what we consider "blank"
        if field_type == "phone":
            # Check if the text is just placeholder characters or very short
            # Remove common placeholder characters and check if it's essentially empty
            cleaned_text = re.sub(r'[\(\)\-\s\.]', '', text_in_area)  # Remove parentheses, dashes, spaces, dots
            is_blank = len(cleaned_text) < 5  # Allow for placeholder text like "( ) -"
            
            # Also check if it contains only placeholder patterns
            placeholder_patterns = [
                r'^[\(\)\-\s\.]+$',  # Only parentheses, dashes, spaces, dots
                r'^\([^)]*\)[^0-9]*$',  # Parentheses with no digits
                r'^[^0-9]*$',  # No digits at all
            ]
            
            for pattern in placeholder_patterns:
                if re.match(pattern, text_in_area):
                    is_blank = True
                    break
        else:
            # For other fields, use the original strict check
            is_blank = len(text_in_area) < 10  # Very strict - almost no text allowed
        
        if is_blank:
            # Calculate placement position with better field boundaries
            placement_x = start_x + 5  # 5 points into the blank area
            # Create a proper field area that's slightly taller than the label
            field_height = max(label_y1 - label_y0, 12)  # Minimum 12 points height
            placement_y = label_y0 - 2  # Start slightly above label baseline
            placement_width = width - 10  # Leave some margin
            placement_height = field_height + 4  # Add some padding
            
            placement_bbox = [placement_x, placement_y, placement_x + placement_width, placement_y + placement_height]
            logger.debug(f"Blank space detected after label at {label_bbox}, placement at {placement_bbox}")
            return True, placement_bbox
    
    logger.debug(f"No blank space detected after label at {label_bbox} in any position")
    return False, []

def is_field_already_filled(page, field_type: str, placement_bbox: List[float]) -> bool:
    """
    Check if a field area already contains valid data for the given field type.
    Returns True if the field appears to be already filled with valid data.
    """
    if field_type not in FIELD_DETECTION_PATTERNS:
        return False
    
    # Get text in the field area
    search_rect = fitz.Rect(placement_bbox[0], placement_bbox[1], placement_bbox[2], placement_bbox[3])
    text_in_area = page.get_text("text", clip=search_rect).strip()
    
    if not text_in_area:
        return False
    
    # Enhanced phone number detection
    if field_type == "phone":
        # First check if the text is just placeholder characters (not a real phone number)
        cleaned_text = re.sub(r'[\(\)\-\s\.]', '', text_in_area)  # Remove parentheses, dashes, spaces, dots
        if len(cleaned_text) < 5:  # If very little text after removing placeholders
            # Check if it contains only placeholder patterns
            placeholder_patterns = [
                r'^[\(\)\-\s\.]+$',  # Only parentheses, dashes, spaces, dots
                r'^\([^)]*\)[^0-9]*$',  # Parentheses with no digits
                r'^[^0-9]*$',  # No digits at all
            ]
            
            for pattern in placeholder_patterns:
                if re.match(pattern, text_in_area):
                    logger.debug(f"Phone field contains placeholder text: '{text_in_area.strip()}' - not filled")
                    return False  # This is placeholder text, not a real phone number
        
        # Check for specific phone number patterns
        for pattern in PHONE_DETECTION_PATTERNS:
            match = re.search(pattern, text_in_area)
            if match:
                logger.info(f"Phone field already contains valid phone number: '{text_in_area.strip()}'")
                return True
        
        # Also check for general digit patterns (at least 7 digits)
        digits = re.findall(r'\d', text_in_area)
        if len(digits) >= 7:
            logger.info(f"Phone field already contains digits: '{text_in_area.strip()}'")
            return True
    
    # Check if the text matches the expected pattern for this field type
    pattern = FIELD_DETECTION_PATTERNS[field_type]
    match = re.search(pattern, text_in_area, re.IGNORECASE)
    
    if match:
        logger.info(f"Field '{field_type}' already contains valid data: '{text_in_area.strip()}'")
        return True
    
    # Special case for email - check if it contains @ symbol
    if field_type == "email" and "@" in text_in_area:
        logger.info(f"Email field already contains @ symbol: '{text_in_area.strip()}'")
        return True
    
    return False

def is_acroform_field_already_filled(field_type: str, current_value: str) -> bool:
    """
    Check if an AcroForm field already contains valid data for the given field type.
    Returns True if the field appears to be already filled with valid data.
    """
    if not current_value or not current_value.strip():
        return False
    
    current_value = current_value.strip()
    
    if field_type not in FIELD_DETECTION_PATTERNS:
        return False
    
    # Enhanced phone number detection for AcroForm fields
    if field_type == "phone":
        # Check for specific phone number patterns
        for pattern in PHONE_DETECTION_PATTERNS:
            match = re.search(pattern, current_value)
            if match:
                logger.info(f"AcroForm phone field already contains valid phone number: '{current_value}'")
                return True
        
        # Also check for general digit patterns (at least 7 digits)
        digits = re.findall(r'\d', current_value)
        if len(digits) >= 7:
            logger.info(f"AcroForm phone field already contains digits: '{current_value}'")
            return True
    
    # Check if the text matches the expected pattern for this field type
    pattern = FIELD_DETECTION_PATTERNS[field_type]
    match = re.search(pattern, current_value, re.IGNORECASE)
    
    if match:
        logger.info(f"AcroForm field '{field_type}' already contains valid data: '{current_value}'")
        return True
    
    # Special case for email - check if it contains @ symbol
    if field_type == "email" and "@" in current_value:
        logger.info(f"AcroForm email field already contains @ symbol: '{current_value}'")
        return True
    
    return False

def check_phone_after_label(page, label_bbox: List[float], page_width: float) -> bool:
    """
    Specifically check if there's already a phone number in the immediate area after a phone label.
    Returns True if a phone number is detected.
    """
    label_x0, label_y0, label_x1, label_y1 = label_bbox
    
    # Check the immediate area after the label (within 200 points)
    search_x0 = label_x1 + 5  # Start 5 points after the label
    search_x1 = min(label_x1 + 200, page_width)  # Check up to 200 points to the right
    search_y0 = label_y0 - 10  # Slightly above the label
    search_y1 = label_y1 + 10  # Slightly below the label
    
    # Get text in the search area
    search_rect = fitz.Rect(search_x0, search_y0, search_x1, search_y1)
    text_in_area = page.get_text("text", clip=search_rect).strip()
    
    if not text_in_area:
        return False
    
    # Check if the text is just placeholder characters (not a real phone number)
    cleaned_text = re.sub(r'[\(\)\-\s\.]', '', text_in_area)  # Remove parentheses, dashes, spaces, dots
    if len(cleaned_text) < 5:  # If very little text after removing placeholders
        # Check if it contains only placeholder patterns
        placeholder_patterns = [
            r'^[\(\)\-\s\.]+$',  # Only parentheses, dashes, spaces, dots
            r'^\([^)]*\)[^0-9]*$',  # Parentheses with no digits
            r'^[^0-9]*$',  # No digits at all
        ]
        
        for pattern in placeholder_patterns:
            if re.match(pattern, text_in_area):
                logger.debug(f"Placeholder text detected after phone label: '{text_in_area.strip()}' - not a real phone number")
                return False  # This is placeholder text, not a real phone number
    
    # Check for specific phone number patterns
    for pattern in PHONE_DETECTION_PATTERNS:
        match = re.search(pattern, text_in_area)
        if match:
            logger.info(f"Phone number detected after label: '{text_in_area.strip()}'")
            return True
    
    # Also check for general digit patterns (at least 7 digits)
    digits = re.findall(r'\d', text_in_area)
    if len(digits) >= 7:
        logger.info(f"Digits detected after phone label: '{text_in_area.strip()}'")
        return True
    
    return False

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
                    # For phone fields, first check if there's already a phone number after the label
                    if field_type == "phone":
                        if check_phone_after_label(page, [x0, y0, x1, y1], page_width):
                            logger.info(f"Phone number already exists after label '{text}', skipping overlay.")
                            continue
                    
                    # Check for blank space after the label
                    is_blank, placement_bbox = detect_blank_space_after_label(page, [x0, y0, x1, y1], page_width, field_type)
                    
                    if is_blank:
                        # Check if the field is already filled
                        if is_field_already_filled(page, field_type, placement_bbox):
                            logger.debug(f"Field '{field_type}' already filled, skipping overlay.")
                            continue

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
        
        page = doc[best_match['page']]
        placement_bbox = best_match['placement_bbox']
        
        # Check if the field is already filled before attempting to fill it
        if is_field_already_filled(page, field_type, placement_bbox):
            logger.info(f"Field '{field_type}' already contains valid data, skipping overlay")
            continue
        
        logger.info(f"Filling '{field_type}' with value '{val}' at position {best_match['placement_bbox']}")
        
        # Get positioning from mapping or use defaults
        entry = next((f for f in mapping.get('fields', []) if f['key'] == field_type), None)
        if entry:
            # For phone numbers, use smaller offset to place them closer to the label
            if field_type == "phone":
                dx = entry['write'].get('offset', {}).get('dx', 5)  # Very close for phone numbers
            else:
                dx = entry['write'].get('offset', {}).get('dx', 50)  # Standard offset for other fields
            dy = entry['write'].get('offset', {}).get('dy', 0)
            size = entry['write'].get('font_size', 10)  # Slightly smaller font for better fit
        else:
            # For phone numbers, use smaller offset to place them closer to the label
            if field_type == "phone":
                dx, dy, size = 5, 0, 10  # Very close for phone numbers
            else:
                dx, dy, size = 50, 0, 10  # Standard offset for other fields
        
        # Calculate text position within the placement area with better alignment
        x = placement_bbox[0] + dx
        # Center the text vertically within the field area
        field_height = placement_bbox[3] - placement_bbox[1]
        y = placement_bbox[1] + (field_height * 0.6) + dy  # Position text in the middle-lower part of the field
        
        # Format text based on field type
        formatted_val = format_field_value(field_type, val)
        
        # Find a safe position that doesn't overlap with existing content
        safe_x, safe_y = find_safe_text_position(page, x, y, formatted_val, size, field_type=field_type)
        
        # Insert text with proper formatting at the safe position
        page.insert_text((safe_x, safe_y), formatted_val, fontname='helv', fontsize=size)
        wrote = True
        
        logger.info(f"Successfully inserted '{formatted_val}' for field '{field_type}' at safe position ({safe_x}, {safe_y})")
    
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

def verify_text_placement(page, x: float, y: float, text: str, fontsize: float = 10) -> bool:
    """
    Verify that placing text at the given position won't overlap with existing content.
    Returns True if placement is safe, False if overlap detected.
    """
    # Estimate text width (rough calculation)
    char_width = fontsize * 0.6  # Approximate character width
    text_width = len(text) * char_width
    text_height = fontsize
    
    # Define the area where text would be placed
    text_bbox = [x, y - text_height, x + text_width, y + 2]
    
    # Check for existing text in this area
    search_rect = fitz.Rect(text_bbox[0], text_bbox[1], text_bbox[2], text_bbox[3])
    existing_text = page.get_text("text", clip=search_rect).strip()
    
    # If there's existing text, placement is not safe
    if existing_text:
        logger.debug(f"Text placement blocked - existing text found: '{existing_text[:20]}...' at position ({x}, {y})")
        return False
    
    return True

def find_safe_text_position(page, base_x: float, base_y: float, text: str, fontsize: float = 10, max_attempts: int = 10, field_type: str = None) -> Tuple[float, float]:
    """
    Find a safe position to place text without overlapping existing content.
    Returns (x, y) coordinates for safe placement.
    """
    char_width = fontsize * 0.6
    text_width = len(text) * char_width
    
    # For phone numbers, try positions closer to the base position
    if field_type == "phone":
        # Try positions very close to the label first
        for attempt in range(max_attempts):
            offset_x = 5 + (attempt * 5)  # Start at 5, increment by 5 each attempt - very close
            test_x = base_x + offset_x
            test_y = base_y
            
            if verify_text_placement(page, test_x, test_y, text, fontsize):
                logger.debug(f"Safe phone position found at ({test_x}, {test_y}) after {attempt + 1} attempts")
                return test_x, test_y
    else:
        # For other fields, use the original logic
        for attempt in range(max_attempts):
            offset_x = 50 + (attempt * 20)  # Start at 50, increment by 20 each attempt
            test_x = base_x + offset_x
            test_y = base_y
            
            if verify_text_placement(page, test_x, test_y, text, fontsize):
                logger.debug(f"Safe text position found at ({test_x}, {test_y}) after {attempt + 1} attempts")
                return test_x, test_y
    
    # If no safe position found, return a position far to the right
    logger.warning(f"No safe position found for text '{text}', placing far to the right")
    return base_x + 300, base_y

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
