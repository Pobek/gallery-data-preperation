# Gallery Data Preperation

A simple script to load `netflix_title` csv file from [kaggle](https://www.kaggle.com/shivamb/netflix-shows/data)

**Currently only works on postgresql**

## Getting Started

- Setup environment variables with `.env` file:

```env
DB_USERNAME={DB_USERNAME}
DB_PASSWORD={DB_PASSWORD}
DB_HOST={DB_HOST}
DB_NAME={DB_NAME}
```
- Install required packages:

```bash
pip3 install -r requirements.txt
```

- Run `main.py`:

```bash
python3 main.py
```