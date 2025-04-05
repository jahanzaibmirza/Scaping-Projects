import csv
import time
from googletrans import Translator

translator = Translator()
def read_input_file():
    with open('sample.csv', 'r', encoding='latin1') as file:
        data = list(csv.DictReader(file))
    return data

def translate_text(text):
    retries = 3
    for attempt in range(retries):
        try:
            # translation = translator.translate(text, src='pl', dest='en')
            translation = translator.translate(text, src='auto', dest='en')
            return translation.text
        except Exception as e:
            print(f"Error translating '{text}': {e}")
            time.sleep(2)
            continue
    return text

def translate_and_save():
    file_data = read_input_file()
    translated_data = []
    for each_row in file_data:
        translated_row = each_row.copy()
        translated_row['URL'] = each_row.get('URL', '')
        translated_row['Company Name'] = translate_text(each_row.get('Company Name', ''))
        translated_row['Company Description'] = translate_text(each_row.get('Company Description', ''))
        translated_row['Company Website'] = each_row.get('Company Website', '')
        translated_row['Company Phone'] = each_row.get('Company Phone', '')
        translated_row['Company Email'] = each_row.get('Company Email', '')
        translated_row['Company Location'] = translate_text(each_row.get('Company Location', ''))

        translated_data.append(translated_row)

    # Write the translated data to a new CSV file
    with open('translated_TSW_data1.csv', 'w', newline='', encoding='utf-8') as file:
        fieldnames = translated_data[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(translated_data)

    print("Translation complete. Data saved in 'translated_TSW_data.csv'.")

translate_and_save()