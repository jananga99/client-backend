# Client Backend

## Overview

The Client Backend project serves as the backend for a client application designed for uploading and retrieving files from a distributed file storage system. This repository provides essential functionalities for interacting with the file storage, including uploading files, retrieving files, and viewing all files. Additionally, the system supports two file types: public and private.

## Setup

### Prerequisites

- Python 3.x installed on your system.
- Ensure pip is installed (Python's package installer).

### Installation

1. Clone the repository to your local machine:

```bash
git clone https://github.com/jananga99/client-backend.git
```

2. Navigate to the project directory:

```bash
cd client-backend
```

3. Create the .env

```bash
cp .env.example .env
```

Fill the required content

4. Create a virtual environment:

```bash
python3 -m venv .venv
```

5. Activate the virtual environment. This step may vary depending on your operating system:

#### For Windows

```bash
.venv\Scripts\activate
```

#### For maxcOS and Linux

```bash
source .venv/bin/activate
```

6. Install the required dependencies:

```bash
pip install -r requirements.txt
```

7. Run the pre.py

```bash
python3 pre.py
```

Alternatively, ```pre.sh``` can be used to automate steps 4,5,6 and 7
```bash
chmod +x ./pre.sh
./pre.sh
```

## Usage

### Running the Backend

To start the backend, run the following command:

```bash
source .venv/bin/activate
python3 index.py
```

Alternatively, ```start.sh``` can be used.

```bash
chmod +x ./start.sh
./start.sh
```

### Interacting with the Backend

Once the backend is running, you can interact with it through HTTP requests. Here are some of the available endpoints:

- Upload File: POST request to /files with the file to be uploaded and accessType.
- Retrieve File: GET request to /files/<file_id> to retrieve a specific file by its ID.
- View All Files: GET request to /files to view all files stored in the system.

## Supported File Types

Currently, the backend supports the following file types for testing purposes:

- Images (e.g., JPEG, PNG, GIF)
- Videos (e.g., MP4, AVI, MOV)
- Text files (e.g., TXT)

## File Access Types

The backend supports two types of files: public and private. When uploading a file, ensure to specify the file type accordingly.
