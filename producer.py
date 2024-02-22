from confluent_kafka import Producer
from time import sleep
import pandas as pd
import random

# Cấu hình producer
producer_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'python-producer'
}

# Khởi tạo producer
producer = Producer(producer_config)

# Chủ đề Kafka mà mình muốn gửi thông điệp đến
topic = 'credit_card_transactions'

# Đọc dữ liệu từ file CSV
csv_file = 'User0_credit_card_transactions.csv'
df = pd.read_csv(csv_file)

# Gửi thông điệp đến chủ đề
while True:
    for index, row in df.iterrows():
        tran = row.to_json()

        # Gửi thông điệp 
        producer.produce(topic, key=str(index), value=tran)
        producer.flush()
        sleep(1)

        print(f'Sent: {tran}')

    # Ngủ trong một khoảng thời gian ngẫu nhiên 
    sleep(random.randint(1, 3))

# Chờ đến khi tất cả các thông điệp được gửi
producer.flush()
print('Messages sent successfully')