import zmq
import os.path
import hashlib
import math
import json

ps = 1024*1024*2
context = zmq.Context()

PathFile = os.path.dirname(os.path.abspath(__file__))

SepDir = "\\"

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def SendSocketMSJ(IpServer,PortServer,MSJ):
    global context
    Path = "tcp://"+str(IpServer)+":"+str(PortServer)
    socket = context.socket(zmq.REQ)
    socket.connect(Path)
    socket.send_multipart(MSJ)
    Msjresponse = socket.recv_multipart()
    socket.close()

    return Msjresponse

def AgregarHashDoc(hashkey,FileName):
    
    WriteinDOC(FileName,hashkey)
    
def AgregarNameDoc(FileName):
    WriteinDOC(FileName,FileName)
    
    
def WriteinDOC(FileName,content):
    
    global PathFile
    global SepDir

    FName = FileName
    FName = FName.split(".")
    FName = FName[0]
    FName = FName+".rf"
    FName = PathFile +SepDir+ FName
    
    f = open(FName, "at")
    f.write(content+"\n")
    f.close()
    

def SendPart(IPTemp,PortTemp,content,hashPart,FileName):

     firstIP = IPTemp
     firstPort = PortTemp

     while True:

        MSJData = SendSocketMSJ(IPTemp,PortTemp,[b"1", content,Strencode(hashPart.hexdigest())])

        if (Bdecode(MSJData[0]) == "1"):
             if (Bdecode(MSJData[1]) == "1"):
                    AgregarHashDoc(Bdecode(MSJData[2]),FileName)
                    return True
             elif (Bdecode(MSJData[1]) == "2"):
                     IPTemp = Bdecode(MSJData[2])
                     PortTemp = Bdecode(MSJData[3])
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

        FileName = os.path.basename(file.name)

        file.seek(0, os.SEEK_END)
        size = file.tell()
        NumParts = math.ceil( size / ps )
        if NumParts == 0 :
            NumParts = 1


        count = 0
        point = 0
        
        AgregarNameDoc(FileName)
        
        while True:

            count = count + 1

            file.seek(int(point))
            content = file.read(ps)
            hashPart  = hashlib.new("sha1",content)

            if (SendPart(NodeIP,NodePort,content,hashPart,FileName)):

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


def AddParttoDoc(content,hash,Filename):

    global PathFile
    global SepDir

    hashPart  = hashlib.new("sha1",content)

    if hash == hashPart.hexdigest():
        PathFileSave =  PathFile +SepDir+ Filename
        archivo = open(PathFileSave,'ab')
        archivo.write(content)
        archivo.close()

    else:
        print("Error integridad archivo")
        exit()

def Download():

    FilePath = input("INGRESE LA RUTA DEL ARCHIVO CON LOS HASH\n")

    if os.path.isfile(FilePath) :

        NodeIP = input("INGRESE LA IP DE ALGUN NODO\n")
        NodeIP = "localhost"
        NodePort = input("INGRESE EL PUERTO DEL NODO\n")

        f = open(FilePath, "rt")

        FileName = ""
        
        count = 0

        while True:

            Hash = f.readline()
            Hash = Hash.replace("\n", "")
            if Hash == "" :
                break
            elif  "." in Hash :
                FileName = Hash
                continue


            IPTemp = NodeIP
            PortTemp = NodePort

            
            count = count + 1
            print("Descargando parte: "+str(count))

            while True:
                
            
                MSJData = SendSocketMSJ(NodeIP,NodePort,[b"2",Strencode(Hash)])
                if (Bdecode(MSJData[0]) == "1"):

                    if (Bdecode(MSJData[1]) == "1"):
                           AddParttoDoc(MSJData[2],Hash,FileName)
                           break

                    elif (Bdecode(MSJData[1]) == "2"):
                            NodeIP = Bdecode(MSJData[2])
                            NodePort = Bdecode(MSJData[3])

                else:
                    print("Error")
                    exit()

                if NodeIP == IPTemp and NodePort == PortTemp :
                    print("Error loop 360")
                    exit()

        print("Archivo descargado")




def Init():

        while True:

            op = Menu()

            if op == 1 :
                Upload()
            elif op == 2:
                Download()



Init()
