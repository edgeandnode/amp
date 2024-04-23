from client import Client

# Convert bytes columns to hex
def to_hex(val):
    return '0x' + val.hex() if isinstance(val, bytes) else val

client = Client("grpc://127.0.0.1:1602")
df = client.get_sql("select * from logs limit 1")
df = df.map(to_hex)
df
