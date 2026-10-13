// ocr_pdf.swift: OCR for scanned PDFs via PDFKit + Vision (macOS only, system frameworks only).
// Usage: ocr_pdf <file.pdf> [--max-pages N] [--dpi N]
// stdout: "--- Page N ---\n<text>" per page (UTF-8). stderr: diagnostics. Exit != 0 on failure.
// Exit codes: 2 cannot open PDF, 3 encrypted, 4 no pages, 5 Vision failure, 64 bad usage.
import Foundation
import PDFKit
import Vision
import AppKit

func eprint(_ s: String) {
    FileHandle.standardError.write((s + "\n").data(using: .utf8)!)
}

var path: String? = nil
var maxPages = 30
var dpi: CGFloat = 200
let args = Array(CommandLine.arguments.dropFirst())
var idx = 0
while idx < args.count {
    switch args[idx] {
    case "--max-pages":
        idx += 1
        if idx < args.count, let v = Int(args[idx]), v > 0 { maxPages = v }
    case "--dpi":
        idx += 1
        if idx < args.count, let v = Double(args[idx]), v >= 72, v <= 400 { dpi = CGFloat(v) }
    default:
        path = args[idx]
    }
    idx += 1
}

guard let pdfPath = path else {
    eprint("usage: ocr_pdf <file.pdf> [--max-pages N] [--dpi N]")
    exit(64)
}
guard let doc = PDFDocument(url: URL(fileURLWithPath: pdfPath)) else {
    eprint("cannot open PDF")
    exit(2)
}
if doc.isLocked {
    eprint("PDF is encrypted")
    exit(3)
}
let total = doc.pageCount
if total == 0 {
    eprint("PDF has no pages")
    exit(4)
}
let limit = min(total, maxPages)

func renderPage(_ page: PDFPage) -> CGImage? {
    let box = page.bounds(for: .mediaBox)
    var w = box.width
    var h = box.height
    if page.rotation % 180 != 0 { swap(&w, &h) }
    let scale = dpi / 72.0
    var size = NSSize(width: w * scale, height: h * scale)
    let maxSide: CGFloat = 6000
    let longest = max(size.width, size.height)
    if longest > maxSide {
        let k = maxSide / longest
        size = NSSize(width: size.width * k, height: size.height * k)
    }
    let img = page.thumbnail(of: size, for: .mediaBox)
    var rect = CGRect(origin: .zero, size: img.size)
    return img.cgImage(forProposedRect: &rect, context: nil, hints: nil)
}

func makeRequest() -> VNRecognizeTextRequest {
    let req = VNRecognizeTextRequest()
    req.recognitionLevel = .accurate
    req.usesLanguageCorrection = true
    let wanted = ["ru-RU", "de-DE", "en-US"]
    if let supported = try? req.supportedRecognitionLanguages() {
        let langs = wanted.filter { supported.contains($0) }
        req.recognitionLanguages = langs.isEmpty ? ["en-US"] : langs
    } else {
        req.recognitionLanguages = wanted
    }
    return req
}

var output = ""
for i in 0..<limit {
    guard let page = doc.page(at: i) else { continue }
    var text = ""
    // Skip rendering when the page already has an extractable text layer? No: this tool is
    // called only for scans, so always OCR.
    if let cg = renderPage(page) {
        let req = makeRequest()
        let handler = VNImageRequestHandler(cgImage: cg, options: [:])
        do {
            try handler.perform([req])
        } catch {
            eprint("Vision failed on page \(i + 1): \(error)")
            exit(5)
        }
        let obs = (req.results ?? []).sorted {
            let ya = $0.boundingBox.midY, yb = $1.boundingBox.midY
            if abs(ya - yb) > 0.008 { return ya > yb }
            return $0.boundingBox.minX < $1.boundingBox.minX
        }
        text = obs.compactMap { $0.topCandidates(1).first?.string }.joined(separator: "\n")
    } else {
        eprint("cannot render page \(i + 1)")
    }
    output += "--- Page \(i + 1) ---\n\(text)\n\n"
    eprint("page \(i + 1)/\(limit) done")
}
if limit < total {
    eprint("truncated: recognized \(limit) of \(total) pages")
    output += "--- TRUNCATED: recognized \(limit) of \(total) pages ---\n"
}
FileHandle.standardOutput.write(output.data(using: .utf8)!)
