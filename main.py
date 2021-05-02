from insert_class import Insert_data
import configparser

def get_creds():
    config = configparser.ConfigParser()
    config.read("settings.ini")
    login = config['login']['login']
    password = config['password']['password']
    database = config['DB']['db_name']
    return [login, password, database]

if __name__ == '__main__':
    creds = get_creds()
    insert_cl = Insert_data(creds)
    insert_cl.insert_to_db('music.csv')