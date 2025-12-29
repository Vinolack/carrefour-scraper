# Carrefour Scraper

## Overview
The Carrefour Scraper is a Python and Node.js based project designed to scrape product links from Carrefour's online store. It reads store links and pagination information from an Excel file, fetches the HTML source code using a JavaScript scraper, and extracts product links for further processing.

## Project Structure
```
carrefour-scraper
├── src
│   ├── scraper.py          # Main logic for the scraping process
│   ├── excel_reader.py     # Reads store links and page numbers from Excel
│   ├── extractor.py        # Extracts product links from HTML source
│   └── node
│       ├── index.js        # JavaScript scraper for fetching HTML
│       └── request.js      # Facilitates HTTP requests
├── input_links.xlsx        # Excel file containing store links and pages
├── requirements.txt         # Python dependencies
├── package.json             # Node.js dependencies and scripts
├── .gitignore               # Files to ignore in Git
└── README.md                # Project documentation
```

## Setup Instructions

### Prerequisites
- Python 3.x
- Node.js and npm

### Installation

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd carrefour-scraper
   ```

2. **Set up Python environment:**
   - It is recommended to use a virtual environment.
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Python dependencies:**
   ```
   pip install -r requirements.txt
   ```

4. **Set up Node.js environment:**
   ```
   cd src/node
   npm install
   ```

## Usage

1. **Prepare the Excel file:**
   - Create or modify `input_links.xlsx` to include the store links and page numbers.

2. **Run the scraper:**
   - Execute the Python script to start the scraping process:
   ```
   python src/scraper.py
   ```

3. **Output:**
   - The extracted product links will be temporarily written to a file for further processing.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.