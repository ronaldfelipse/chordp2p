import zmq
import os.path
import hashlib
import math


def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def SendSocketMSJ(IpServer,PortServer,MSJ,Globals):

    Path = "tcp://"+str(IpServer)+":"+str(PortServer)
    socket = Globals["context"].socket(zmq.REQ)
    socket.connect(Path)
    socket.send_multipart(MSJ)
    Msjresponse = socket.recv_multipart()
    socket.close()

    return Msjresponse

def AgregarHashDoc(hashkey,FileName,Globals):
    
    WriteinDOC(FileName,hashkey,Globals)
    
def AgregarNameDoc(FileName,Globals):
    WriteinDOC(FileName,FileName,Globals)
    
    
def WriteinDOC(FileName,content,Globals):


    FName = FileName
    FName = FName.split(".")
    FName = FName[0]
    FName = FName+".rf"
    FName = Globals["PathFile"] +Globals["SepDir"]+ FName
    
    f = open(FName, "at")
    f.write(content+"\n")
    f.close()
    

def SendPart(IPTemp,PortTemp,content,hashPart,FileName,Globals):

     firstIP = IPTemp
     firstPort = PortTemp

     while True:

        MSJData = SendSocketMSJ(IPTemp,PortTemp,[b"1", content,Strencode(hashPart.hexdigest())],Globals)

        if (Bdecode(MSJData[0]) == "1"):
             if (Bdecode(MSJData[1]) == "1"):
                    AgregarHashDoc(Bdecode(MSJData[2]),FileName,Globals)
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


def Upload(Globals):

    FilePath = input("INGRESE LA RUTA DEL ARCHIVO A SUBIR\n")

    if os.path.isfile(FilePath) :

        NodeIP = input("INGRESE LA IP DE ALGUN NODO\n")
        NodePort = input("INGRESE EL PUERTO DEL NODO\n")

        file = open(FilePath, "rb")

        FileName = os.path.basename(file.name)

        file.seek(0, os.SEEK_END)
        size = file.tell()
        NumParts = math.ceil( size / Globals["ps"] )
        if NumParts == 0 :
            NumParts = 1


        count = 0
        point = 0
        
        AgregarNameDoc(FileName,Globals)
        
        while True:

            count = count + 1

            file.seek(int(point))
            content = file.read(Globals["ps"])
            hashPart  = hashlib.new("sha1",content)

            if (SendPart(NodeIP,NodePort,content,hashPart,FileName,Globals)):

                print("Enviando parte "+str(count)+" de "+ str(NumParts))


                if (point + Globals["ps"]) >= size:
                    break
                else:
                    point = point + Globals["ps"]

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


def AddParttoDoc(content,hash,Filename,Globals):

    hashPart  = hashlib.new("sha1",content)

    if hash == hashPart.hexdigest():
        PathFileSave =  Globals["PathFile"] +Globals["SepDir"]+ Filename
        archivo = open(PathFileSave,'ab')
        archivo.write(content)
        archivo.close()

    else:
        print("Error integridad archivo")
        exit()

def Download(Globals):

    FilePath = input("INGRESE LA RUTA DEL ARCHIVO CON LOS HASH\n")

    if os.path.isfile(FilePath) :

        NodeIP = input("INGRESE LA IP DE ALGUN NODO\n")
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
                
            
                MSJData = SendSocketMSJ(NodeIP,NodePort,[b"2",Strencode(Hash)],Globals)
                if (Bdecode(MSJData[0]) == "1"):

                    if (Bdecode(MSJData[1]) == "1"):
                           AddParttoDoc(MSJData[2],Hash,FileName,Globals)
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
    
        Globals = {}
        
        Globals["ps"] = 1024*1024*2
        Globals["context"] = zmq.Context()
        Globals["PathFile"] = os.path.dirname(os.path.abspath(__file__))
        Globals["SepDir"] = "\\"

        while True:

            op = Menu()

            if op == 1 :
                Upload(Globals)
            elif op == 2:
                Download(Globals)



Init()
