# FIRC-LLM-Extractor
This project automates the end-to-end processing of inward remittance and FIRC (Foreign Inward Remittance Certificate) notifications received via Gmail.
It continuously polls your Gmail inbox, classifies new messages using customizable rules, extracts financial data from both email bodies and bank-issued PDF attachments using OpenAI models, uploads relevant PDFs to Google Drive, and stores the structured data in a central Excel file.

    Automated Gmail Polling – Connects to Gmail API to fetch only new, unprocessed emails.

    Rule-Based Categorization – Uses rules.py to detect:

        Disposal Required inward remittance notifications.

        FIRC / Debit-Cum-Credit Advice PDFs from HDFC, Yes Bank, etc.

    Bank-Agnostic Financial Extraction –

        Email text parsed and normalized (disposal_handler.py).

        PDF attachments decrypted (if necessary), converted to text, and processed (firc_handler.py).

        Uses OpenAI GPT models to extract transaction details (amounts, currencies, dates, remitter/beneficiary info, purpose codes, etc.).

    Password Handling for PDFs – Automatically detects and tries passwords from:

        Rules metadata.

        Environment variables.

        passwords.json mapping.

        Hints found in the email body.

    Google Drive Integration – Uploads original or decrypted PDFs to a dedicated Drive folder.

    Excel Output – All extracted financial data upserted into remittance.xlsx, keyed by unique transaction references.

    Hot-Reload Rules – runner.py reloads categorization rules each polling cycle without restart.

File Structure

    runner.py – Main loop for Gmail polling, message fetching, and handler dispatch.

    rules.py – Email classification rules and attachment filters.

    handlers/disposal_handler.py – Processes inward remittance disposal emails via LLM extraction.

    handlers/firc_handler.py – Processes FIRC/Advice PDFs, including password decryption and Drive upload.

    remittance.xlsx – Persistent store for structured transaction records.

    passwords.json (optional) – Sender/domain/subject-based PDF password hints.

Requirements

    Python 3.9+

    Google API client libraries (google-api-python-client, google-auth-oauthlib, etc.)

    OpenAI Python SDK

    PDF processing libraries (PyPDF2, pdfplumber, pikepdf)

    Pandas & OpenPyXL

    Valid Gmail & Google Drive API credentials (credentials.json)

    OpenAI API key (OPENAI_API_KEY)
