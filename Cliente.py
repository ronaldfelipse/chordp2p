import zmq
import os.path
import hashlib
import json

ps = 0
context = zmq.Context()

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def SendSocketMSJ(IpServer,PortServer,MSJ):
    global context
    Path = "tcp://"+IpServer+":"+PortServer
    socket = context.socket(zmq.REQ)
    socket.connect(Path)
    socket.send_multipart(MSJ)
    Msjresponse = socket.recv_multipart()
    socket.close()

    return Msjresponse

def AgregarHashDoc(hashkey):
    print(hashkey)

def SendPart(IPTemp,PortTemp,content,hashPart):
    
     firstIP = IPTemp
     firstPort = PortTemp
     
     while True:
         
        MSJData = SendSocketMSJ(IPTemp,PortTemp,[b"1", content,Strencode(hashPart.hexdigest())])
                  
        if (Bdecode(MSJData[0]) == "1"):
             if (Bdecode(MSJData[1]) == "1"):
                    AgregarHashDoc(Bdecode(MSJData[2]))
                    return True
             elif (Bdecode(MSJData[1]) == "2"):
                     IPTemp = MSJData[2]
                     PortTemp = MSJData[3]
        else:
            print("Error interno 1"+Bdecode(MSJData[1]))
            return False 
             
        if firstIP == IPTemp and firstPort == PortTemp :
            print("Error loop 360")
            return False 
    

def Upload():

    global ps

    FilePath = input("INGRESE LA RUTA DEL ARCHIVO A SUBIR\n")

    if os.path.isfile(FilePath) :
        
        NodeIP = input("INGRESE LA IP DE ALGUN NODO\n")
        NodeIP = "localhost"
        NodePort = input("INGRESE EL PUERTO DEL NODO\n")

        file = open(FilePath, "rb")

        file.seek(0, os.SEEK_END)
        size = file.tell()
        NumParts =round ( size / ps )
        if NumParts == 0 :
            NumParts = 1


        count = 0
        point = 0
        while True:

            count = count + 1

            file.seek(int(point))
            content = file.read(ps)
            hashPart  = hashlib.new("sha1",content)
           
            if (SendPart(NodeIP,NodePort,content,hashPart)):

                print("Enviando parte "+str(count)+" de "+ str(NumParts))


                if (point + ps) >= size:
                    break
                else:
                    point = point + ps

            else:
                print("Error")
                break


        file.close()


    else:
        print("El archivo no existe")
        

def Menu():
    
  while True:
      
    print("Menu \n1.Upload\n2.Download\n")
    op =  int(input("INGRESE LA OPCIÓN\n"))
    if op == 1:
        return 1
    elif  op == 2:
        return 2
    else:
        print("Opcion invalida")
        
def Init():

        while True:

            op = Menu()

            if op == 1 :
                Upload()
            elif op == 2:
                Download()
        


Init()
