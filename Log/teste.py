import logging

logging.basicConfig(level=logging.INFO,filename="log",format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s")

def calculo_fcr (fcr,chamadas):
    if fcr > chamadas:
        logging.warning(f'O volume das fcr {fcr} esta menor que o total de chamadas {chamadas}')
    return (fcr/chamadas)*100

def calculo_csat(promotores,detratores):
    return (promotores/(detratores + promotores))*100

fcr = 1260
chamadas = 1250
promotores = 750
detratores = 350



calculo_fc = calculo_fcr(fcr,chamadas)
logging.info(f'O resultado de fcr e : {calculo_fc}')
calculo_csa=calculo_csat(promotores,detratores)
logging.info(f'O resultado de CSAT e: {calculo_csa}')
