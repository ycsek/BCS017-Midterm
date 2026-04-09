import csv

headers = ['product_id', 'category', 'price', 'quantity', 'rating']

with open('sales_data.txt', 'r', encoding='utf-8') as txt_file:
    with open('sales_data.csv', 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        for line in txt_file:
            row = [field.strip() for field in line.strip().split(',')]
            if row and len(row) == 5:
                writer.writerow(row)

print("Finished. Saved as sales_data.csv")
