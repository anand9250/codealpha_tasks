import requests
BASE = 'http://localhost:8000'

def register():
    r = requests.post(BASE + '/register', json={'username':'alice','password':'StrongPass!234','ssn':'111-22-3333'})
    print('register', r.status_code, r.json())

def login():
    r = requests.post(BASE + '/login', json={'username':'alice','password':'StrongPass!234'})
    print('login', r.status_code, r.json())
    return r.json().get('access_token')

def select_user(token):
    r = requests.post(BASE + '/query', headers={'X-Capability': token}, json={'operation':'select_user','params':{}})
    print('select', r.status_code, r.json())

def update_ssn(token):
    r = requests.post(BASE + '/query', headers={'X-Capability': token}, json={'operation':'update_ssn','params':{'new_ssn':'999-88-7777'}})
    print('update', r.status_code, r.json())

if __name__ == '__main__':
    register()
    tok = login()
    if tok:
        select_user(tok)
        update_ssn(tok)
        select_user(tok)
