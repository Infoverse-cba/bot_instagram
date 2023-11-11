import logging
import time
import psycopg2
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait 
from time import sleep

class bot_face():
    def __init__(self, cred_login, cred_senha):
        options = webdriver.FirefoxOptions()
        options.add_argument("-headless")

        # self.driver = webdriver.Firefox(options=options)
        self.driver = webdriver.Firefox()
        self.cred_login = cred_login
        self.cred_senha = cred_senha
        sleep(3)

    def time_out(void=None, time_out: int = 20, raise_exception: bool = True):


        """Executes a function with a timeout limit.

        :param void: (optional) Default argument, unused.
        :type void: any
        :param time_out: The timeout limit in seconds.
        :type time_out: int
        :param raise_exception: (optional) If True, a TimeoutException will be raised when the timeout is reached.
        :type raise_exception: bool
        :return: Returns the result of the executed function.
        :rtype: any

        Example:
            This decorator can be used to set a timeout limit for a function that takes too long to execute.
            >>>@time_out(time_out=30, raise_exception=True)
            >>>def slow_function():
            >>>    time.sleep(35)
            >>>
            >>>slow_function()
            TimeoutException: Timeout!"""
    

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                # print("Time out value: {}".format(time_out))
                contadortime_out = 0
                ret = False
                error = None
                while contadortime_out < time_out:
                    try:
                        ret = func(*args, **kwargs)
                        break
                    except Exception as e:
                        logging.exception(e) # serve para salvar o erro no log
                        error = e
                        time.sleep(1)
                    contadortime_out += 1
                if contadortime_out >= time_out and raise_exception:
                    raise error
                return ret

            return inner_wrapper

        return wrapper

    def login(self):
        print('Fazendo login no Instagram...')

        self.driver.get('https://www.instagram.com/accounts/login/')
        self.driver.maximize_window()

        sleep(3)

        self.driver.find_element(by=By.NAME, value='username').send_keys(self.cred_login)
        self.driver.find_element(by=By.NAME, value='password').send_keys(self.cred_senha)
        self.driver.find_element(by=By.XPATH, value='//*[@id="loginForm"]/div/div[3]/button/div').click()

        sleep(3)

        # self.driver.get('https://www.instagram.com/')

        try:
            self.driver.find_element(by=By.XPATH, value='/html/body/div[4]/div/div/div/div[3]/button[2]').click()

        except:
            pass

        self.driver.implicitly_wait(10)

    @time_out(time_out=10, raise_exception=True)
    def search_keyword(self, keyword):
        print(f'Pesquisando por: {keyword}...')
        sleep(4)
        self.driver.get('https://www.facebook.com/search/posts?q='+keyword)

    @time_out(time_out=10, raise_exception=False)
    def get_post_links(self, n_posts=20):
        print('Obtendo links dos posts...')
        self.post_links = list()
        sleep(10)
        n_scroll = 0

        while True:
            script_n_posts = f""" 
                                var n_posts = document.getElementsByClassName('x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z').length
                                return n_posts
                              """
            
            n_posts_browser = self.driver.execute_script(script_n_posts)

            if n_posts_browser >= n_posts:
                n_posts_browser = n_posts
                break

            elif n_scroll > 50:
                print(f"foram encontrados o total de {n_posts_browser} posts de {n_posts}")
                
                break

            else:
                n_scroll += 1
                self.driver.execute_script("window.scrollBy(0,6150)")
                sleep(1)


        # self.driver.execute_script("window.scrollBy(0,6150)")

        script = f""" 
                    var results = document.getElementsByClassName('x1i10hfl xjbqb8w x6umtig x1b1mbwd xaqea5y xav7gou x9f619 x1ypdohk xt0psk2 xe8uvvx xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x16tdsg8 x1hl2dhg xggy1nq x1a2a7pz x1heor9g xt0b8zv xo1l8bm')
                    return results
                  """
        
        elements = self.driver.execute_script(script)
        elements = elements[:n_posts_browser]
        
        self.driver.execute_script("window.scrollBy(0,-"+ str(n_scroll*6150) +")")


        for element in elements:
            try:
                href = element.get_attribute('href')
                if '#' in href:
                    element.click()
                    href = element.get_attribute('href')
                else: continue
                
            except:
                self.driver.execute_script("window.scrollBy(0,1150)")
                sleep(1)
                element.click()
                sleep(1)
                href = element.get_attribute('href')

            if href not in self.post_links:
                self.post_links.append(href)

            if len(self.post_links) == n_posts:
                break

        print('numero de post_links: ', len(self.post_links))
        
    def get_data(self):
        return self.data

    @time_out(time_out=10, raise_exception=False)
    def get_information(self):
        print('Tirando screenshots...')

        info = list()

        for i,link in enumerate(self.post_links):
            self.driver.get(link)
            sleep(2)
            self.driver.save_screenshot('imgs/'+str(i)+'.png')

            info.append([link, link])

        self.data = pd.DataFrame(info, columns=['link', 'publication_id'])

def execute_sql(sql, data = None, fetch=False):
    try:
        con = conecta_db()
        cursor = con.cursor()

        if fetch:
            cursor.execute(sql)
            rows = cursor.fetchall()
            con.commit()
            cursor.close()
            con.close()

            return rows
        
        cursor.execute(sql, data)
        con.commit()

        cursor.close()
        con.close()

    except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
                    con.rollback()
                    cursor.close()
                    con.close()
                    raise(error)

def conecta_db():
    con = psycopg2.connect(host='db.infoverse.com.br', 
                            database='infoverse',
                            user='infoverse', 
                            password='fMCTSepyEXpH')
    return con
        
def retorna_pesquisa_avulsa():
    sql = """SELECT id, id_usuario, id_credencial, data_pesquisa, rede_social, status, palavra_chave, filtro, filtro_avancado, ano_referencia, publicacoes_de, localizacao_marcada
            FROM pesquisa_avulsa
            WHERE status IS NULL OR status = False;"""
    
    rows = execute_sql(sql, fetch=True)

    return rows
    
def set_status_pesquisa_avulsa(id):
    sql = """UPDATE pesquisa_avulsa
            SET status=true
            WHERE id ="""+ str(id) +""";"""
    
    execute_sql(sql)

def retorna_credencial(credencial_id):
    sql = """SELECT id, descricao, usuario, senha
    FROM bot_credencial_facebook WHERE id ="""+ str(credencial_id) +""";"""

    row = execute_sql(sql, fetch=True)

    return row
    
def verificando_busca_avulsa():
    rows = retorna_pesquisa_avulsa()

    for row in rows:
        id, id_usuario, id_credencial, date_search, rede_social, status, keyword, filtro, filtro_avancado, ano_referencia, publicacoes_de, localizacao_marcada = row

        row2 = retorna_credencial(id_credencial)
        _, _, cred_usuario, cred_senha = row2[0]

        executar_busca(id, cred_usuario, cred_senha, keyword)
        set_status_pesquisa_avulsa(id)

def executar_busca(id, cred_login, cred_senha, keyword):
    print('executando busca...')

    bot = bot_face(cred_login, cred_senha)
    bot.login()

    sleep(5)

    # bot.search_keyword(keyword)
    # bot.get_post_links()
    # bot.get_information()

    # inserir_db(bot.get_data(), id)
       
def inserir_db(data, id):
    print('Inserindo no banco de dados...')

    for i,link in enumerate(data['link']):
        try:
    
            publication_id = link
            publication_id = remover_letra(publication_id, '/')
            publication_id = remover_letra(publication_id, ':')
            publication_id = remover_letra(publication_id, '?')
            publication_id = remover_letra(publication_id, ',')
            publication_id = remover_letra(publication_id, '.')
            publication_id = remover_letra(publication_id, '=')
            publication_id = remover_letra(publication_id, '[')
            publication_id = remover_letra(publication_id, ']')
            publication_id = remover_letra(publication_id, '_')
            publication_id = remover_letra(publication_id, '-')
            publication_id = remover_letra(publication_id, '%')
            publication_id = remover_letra(publication_id, '#')
            publication_id = remover_letra(publication_id, '&')
            publication_id = remover_letra(publication_id, '!')
            publication_id = remover_letra(publication_id, '(')
            publication_id = remover_letra(publication_id, ')')

            sql = """
            INSERT into contigencia (link_publication, publication_id, id_pesquisa_avulsa) 
            values('%s','%s', '%s');
            """ % (data['link'][i], publication_id, id)

            linhas = execute_sql("""SELECT publication_id FROM contigencia WHERE publication_id = '"""+ str(publication_id) +"""';""", fetch=True)
            
            # Conte o número de linhas retornadas
            numero_de_linhas = len(linhas)

            if numero_de_linhas == 0:
                execute_sql(sql)

                with open('imgs/'+str(i)+'.png', 'rb') as file:
                    imagem_bytes = file.read()


                data_img = (publication_id, psycopg2.Binary(imagem_bytes))

                sql2 = """
                        INSERT INTO pesquisa_screenshot (publication_id, bytea) 
                        VALUES (%s, %s);
                        """
                execute_sql(sql2, data_img)

        except Exception as e:
            print('Erro na insersão de dados')
            raise(e)

    print('Inserido com sucesso!')

def remover_letra(string, letra_retirar):
    nova_string = ""
    for letra in string:
        if letra != letra_retirar:
            nova_string += letra
    return nova_string

if __name__ == '__main__':
    global precessando 
    processando = False
    print('Verificando busca avulsa')
    
    # while True:
    cred_login = 'vitor_custodio22'
    cred_senha = 'vitorjipa22'
    time.sleep(10)
    # verificando_busca_avulsa()
    executar_busca(1, cred_login, cred_senha, 'amor')



