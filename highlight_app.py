import io
import zipfile
from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import re
import tempfile

app = Flask(__name__)
app.secret_key = "highlight-secret"

# Common words/phrases that might need highlighting
COMMON_HIGHLIGHTS = [
    # Signature-related fields
    "Signature of Claimant", "Claimant Signature", "Claimant's Signature",
    "Signature of Notary", "Notary Public Signature", "Notary Signature",
    "Notary Public", "Notary", "Notarized", "Notarization",
    "Witness Signature", "Witness", "Witnessed",
    "Authorized Signature", "Authorized Signer", "Authorized Representative",
    "Legal Signature", "Legal Representative", "Legal Guardian",
    "Power of Attorney", "POA", "Attorney Signature"
]

def highlight_pdf(pdf_bytes, highlight_words):
    """Highlight specified words in a PDF by finding their positions and drawing yellow rectangles"""
    try:
        # Try to import PyMuPDF (fitz) for better text highlighting
        try:
            import fitz  # PyMuPDF
            use_fitz = True
        except ImportError:
            use_fitz = False
            print("PyMuPDF not available, using fallback method")
        
        if use_fitz:
            # Use PyMuPDF for better text highlighting
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                
                # Search for each word on the page
                for word in highlight_words:
                    # Search for the word (case insensitive)
                    text_instances = page.search_for(word, flags=fitz.TEXTFLAGS_IGNORECASE)
                    
                    # Highlight each instance found
                    for inst in text_instances:
                        # Create a highlight annotation
                        highlight = page.add_highlight_annot(inst)
                        highlight.set_colors(stroke=[1, 1, 0])  # Yellow
                        highlight.set_opacity(0.3)  # Semi-transparent
                        highlight.update()
            
            # Save the highlighted PDF
            output_stream = io.BytesIO()
            doc.save(output_stream, garbage=4, deflate=True)
            doc.close()
            output_stream.seek(0)
            return output_stream.getvalue()
        
        else:
            # Fallback method using PyPDF2
            import PyPDF2
            from PyPDF2 import PdfReader, PdfWriter
            
            pdf_reader = PdfReader(io.BytesIO(pdf_bytes))
            pdf_writer = PdfWriter()
            
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                
                # Extract text from the page
                text = page.extract_text()
                
                # For each word to highlight, create a simple text-based annotation
                for word in highlight_words:
                    if word.lower() in text.lower():
                        # Create a simple highlight annotation
                        # This is a basic fallback - won't be as precise as PyMuPDF
                        highlight_annotation = {
                            '/Type': '/Annot',
                            '/Subtype': '/Highlight',
                            '/Rect': [50, 750 - (page_num * 20), 200, 770 - (page_num * 20)],
                            '/F': 4,
                            '/C': [1, 1, 0],  # Yellow
                            '/T': 'Highlight',
                            '/Contents': f'Found: {word}'
                        }
                        
                        if '/Annots' not in page:
                            page['/Annots'] = []
                        page['/Annots'].append(highlight_annotation)
                
                pdf_writer.add_page(page)
            
            output_stream = io.BytesIO()
            pdf_writer.write(output_stream)
            output_stream.seek(0)
            return output_stream.getvalue()
            
    except Exception as e:
        print(f"Error highlighting PDF: {e}")
        # Return original PDF if highlighting fails
        return pdf_bytes

def process_highlight_zip(zip_bytes, highlight_words):
    """Process a ZIP file and highlight specified words in all PDFs"""
    output_zip = io.BytesIO()
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as input_zip:
        with zipfile.ZipFile(output_zip, 'w') as output_zip_file:
            for file_info in input_zip.filelist:
                if file_info.filename.lower().endswith('.pdf'):
                    # Read the PDF
                    pdf_bytes = input_zip.read(file_info.filename)
                    
                    # Highlight the PDF
                    highlighted_pdf = highlight_pdf(pdf_bytes, highlight_words)
                    
                    # Add to output ZIP
                    output_zip_file.writestr(f"highlighted_{file_info.filename}", highlighted_pdf)
                else:
                    # Copy non-PDF files as-is
                    file_bytes = input_zip.read(file_info.filename)
                    output_zip_file.writestr(file_info.filename, file_bytes)
    
    return output_zip.getvalue()

@app.route("/", methods=["GET"])
def index():
    return render_template("highlight_index.html", common_highlights=COMMON_HIGHLIGHTS)

@app.route("/highlight", methods=["POST"])
def highlight():
    # Get selected highlight words
    selected_words = request.form.getlist("highlight_words")
    custom_words = request.form.get("custom_words", "").strip()
    
    # Combine selected and custom words
    all_highlight_words = selected_words.copy()
    if custom_words:
        # Split custom words by comma or newline
        custom_list = re.split(r'[,;\n]', custom_words)
        all_highlight_words.extend([word.strip() for word in custom_list if word.strip()])
    
    if not all_highlight_words:
        flash("Please select at least one word to highlight.")
        return redirect(url_for("index"))
    
    f = request.files.get("zipfile")
    if not f or not f.filename.lower().endswith(".zip"):
        flash("Please upload a .zip file containing PDFs.")
        return redirect(url_for("index"))
    
    zip_bytes = f.read()
    
    try:
        highlighted_zip = process_highlight_zip(zip_bytes, all_highlight_words)
        
        return send_file(
            io.BytesIO(highlighted_zip),
            mimetype="application/zip",
            as_attachment=True,
            download_name="highlighted_documents.zip"
        )
    except Exception as e:
        flash(f"Error processing documents: {str(e)}")
        return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
