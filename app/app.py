
import io
from flask import Flask, render_template, request, send_file, flash, redirect, url_for
from processor import process_zip

app = Flask(__name__)
app.secret_key = "dev-secret"

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    name = request.form.get("name","").strip()
    email = request.form.get("email","").strip()
    phone = request.form.get("phone","").strip()
    address = request.form.get("address","").strip()
    ein = request.form.get("ein","").strip()

    f = request.files.get("zipfile")
    if not f or not f.filename.lower().endswith(".zip"):
        flash("Please upload a .zip file containing PDFs.")
        return redirect(url_for("index"))

    zip_bytes = f.read()
    values = {"name": name, "email": email, "phone": phone, "address": address, "ein": ein}

    out_zip = process_zip(zip_bytes, values)

    return send_file(
        io.BytesIO(out_zip),
        mimetype="application/zip",
        as_attachment=True,
        download_name="processed_pdfs.zip"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

