import tdma

client = tdma.Client()
client.new_credentials(refresh=False)
client.save_credentials()