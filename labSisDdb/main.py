from nodeCoord import NodeCoord 
import asyncio

def conexaoSocket(node_id:int, host:str, port:int, listaAdicionar:list):
    conexao = NodeCoord(node_id,host, port)
    try:
        print(conexao.get_socket().)
    except ConnectionRefusedError:
        return None
    listaAdicionar.append(conexao)
    return conexao
    

async def main():
    lista_nos = []
    maquina_1:NodeCoord | None = conexaoSocket(0,'localhost',3000, lista_nos) #NodeCoord(0,'localhost',3000)
    maquina_2:NodeCoord | None = conexaoSocket(1,'localhost',3001, lista_nos) #NodeCoord(1,'localhost',3001)
    maquina_3:NodeCoord | None = conexaoSocket(2,'localhost',3002, lista_nos) #NodeCoord(2,'localhost',3002)
    print(lista_nos)
    while True:
      lista_nos[0].iniciarEleicao(lista_nos)
      await asyncio.sleep(2)
      lista_nos_copia:list = []
      maquina_1:NodeCoord | None = conexaoSocket(0,'localhost',3000, lista_nos_copia) #NodeCoord(0,'localhost',3000)
      maquina_2:NodeCoord | None = conexaoSocket(1,'localhost',3001, lista_nos_copia) #NodeCoord(1,'localhost',3001)
      maquina_3:NodeCoord | None = conexaoSocket(2,'localhost',3002, lista_nos_copia) #NodeCoord(2,'localhost',3002)
      lista_nos = lista_nos_copia

    #print(maquina_1.get_socket())

if __name__ == "__main__":
    asyncio.run(main())


