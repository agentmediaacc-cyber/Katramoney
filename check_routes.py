from app import app
print('--- Current Flask Routes ---')
for rule in app.url_map.iter_rules():
    print(f'Path: {rule.rule} --> Function: {rule.endpoint}')
