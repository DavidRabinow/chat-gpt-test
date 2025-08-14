import os, io, zipfile, shutil, tempfile, yaml
from pathlib import Path
from typing import Dict, List
from loguru import logger
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter

ROOT = Path(__file__).resolve().parent
PATTERNS = yaml.safe_load(open(ROOT / 'config' / 'patterns.yaml', 'r', encoding='utf-8'))
MAPPING = yaml.safe_load(open(ROOT / 'config' / 'mapping.yaml', 'r', encoding='utf-8'))

def process_zip(zip_bytes: bytes, values: Dict[str, str]) -> bytes:
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
            out_pdf = out_dir / f"filled_{pdf.name}"
            ok = fill_pdf(pdf, out_pdf, values)
            if not ok:
                import shutil
                shutil.copy2(pdf, out_dir / f"original_{pdf.name}")

        mem = io.BytesIO()
        with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as zfo:
            for p in out_dir.iterdir():
                zfo.write(p, arcname=p.name)
        mem.seek(0)
        return mem.read()

def fill_pdf(src_path: Path, dst_path: Path, values: Dict[str, str]) -> bool:
    if detect_acroform_fields(src_path):
        aliases = {f['key']: f.get('acroform_names', []) for f in MAPPING['fields']}
        ok = fill_acroform(src_path, dst_path, values, aliases)
        if ok:
            return True
    anchors = search_labels_positions(src_path, PATTERNS['labels'])
    ok2 = overlay_values(src_path, dst_path, anchors, values, MAPPING)
    return ok2

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
    for logical_key, aliases in field_aliases.items():
        val = values.get(logical_key)
        if not val:
            continue
        for name in (fields.keys() if fields else []):
            if name in aliases:
                update_map[name] = val
    if not update_map:
        return False
    writer.update_page_form_field_values(writer.pages[0], update_map)
    for j in range(len(writer.pages)):
        page = writer.pages[j]
        if '/Annots' in page:
            del page['/Annots']
    with open(out_path, 'wb') as fw:
        writer.write(fw)
    return True

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
