import zmq
import random
import hashlib
import os


def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")


def ip_puerto(Globals):
       
        Globals["MyIP"] = input("INGRESE LA IP del server\n")
       
        Globals["MyPort"] = input("INGRESE PUERTO del server\n")


def SendSocketMSJ(IpServer,PortServer,MSJ,Globals):
     
    Path = "tcp://"+IpServer+":"+PortServer
    socket = Globals["context"].socket(zmq.REQ)
    socket.connect(Path)
    socket.send_multipart(MSJ)
    Msjresponse = socket.recv_multipart()
    socket.close()

    return Msjresponse

def CheckFirstNode():

    while True:

        CheckFirst = input("ES EL PRIMER NODO? (Y/N)\n")

        if (CheckFirst.upper() == "Y"):
            return 1
        elif  (CheckFirst.upper() == "N"):
            return 0
        else :
            print("Opcion invalida")

def inToAnillo(Globals):

    FirstIP = input("INGRESE LA IP DE ALGUN NODO\n")
    FirstPort = input("INGRESE EL PUERTO DEL NODO\n")

    while True:
        MSJData = SendSocketMSJ(FirstIP,FirstPort,[b'0',Strencode(Globals["ServerID"]),Strencode(Globals["MyIP"]),Strencode(Globals["MyPort"])],Globals)
        if (Bdecode(MSJData[0]) != "1"):
            print("Error conectandose con el nodo")
            return 0
        else:
            if (Bdecode(MSJData[1]) == "1"):
                Globals["IDInit"] =   Bdecode(MSJData[2])
                Globals["AntecesorIP"] = Bdecode(MSJData[3])
                Globals["AntecesorPort"] = Bdecode(MSJData[4])
                break
            else:

                FirstIP = Bdecode(MSJData[2])
                FirstPort = Bdecode(MSJData[3])

    Globals["IDFin"] = Globals["ServerID"]
    Count = 0
    
    while True:
        MSJData = SendSocketMSJ(FirstIP,FirstPort,[b'3',Strencode(Globals["IDInit"]),Strencode(Globals["IDFin"])],Globals)
        if (Bdecode(MSJData[0]) == "2"):
            Count = Count + 1
            print ("Trasfiriendo archivo "+str(Count))
            path =  Globals["PathFile"] +"/"+ Bdecode(MSJData[2])
            archivo = open(path,'ab')
            archivo.write(MSJData[1])
            archivo.close()
            
        else:
            break
        
    
    PrintMyRange(Globals)

def FirstConect(Globals):

    if not CheckFirstNode():

        inToAnillo(Globals)

    else:

        if int(Globals["ServerID"]) == 2**160 :
            Globals["IDInit"] = 0
        else:
            Globals["IDInit"] = int(Globals["ServerID"])+1

        Globals["IDFin"] = Globals["ServerID"]

    return 1

def Checkintervale(inicio,Fin,Value):

    if int(inicio) > int(Fin) :
        return (int(Value) <= int(Fin)  and int(Value) >= 0) or ( int(Value) <= 2**160 and int(Value) >= int(inicio))
    else:
        return (int(Value) >= int(inicio)  and int(Value) <= int(Fin))


def getID(Globals):
 
    Random = random.randint(0,9876543219876543219876543) #25
    hash  = hashlib.new("sha1",Strencode(Random))
    Globals["ServerID"] = int(hash.hexdigest(), 16)
    print("ServerID: "+str(Globals["ServerID"]))

def CheckNewID(NewServerID,IP,PORT,Globals):
    

    if Checkintervale(Globals["IDInit"],Globals["IDFin"],NewServerID) :

        IdInicopy = Globals["IDInit"]

        if int(NewServerID) == 2**160 :
            Globals["IDInit"] = 0
        else:
            Globals["IDInit"] = int(NewServerID)+1

        if Globals["AntecesorIP"] ==  "":
             respt = [b"1",b"1",Strencode(IdInicopy),Strencode(Globals["MyIP"]),Strencode(Globals["MyPort"])]

        else:
             respt = [b"1",b"1",Strencode(IdInicopy),Strencode(Globals["AntecesorIP"]),Strencode(Globals["AntecesorPort"])]

        Globals["AntecesorIP"]    = IP
        Globals["AntecesorPort"]   = PORT
        PrintMyRange(Globals)
        return respt
    else:
        return [b"1",b"2",Strencode(Globals["AntecesorIP"]),Strencode(Globals["AntecesorPort"])]

def PrintMyRange(Globals):


    print("-----------------------------------------------------")
    print("IDIni = " + str(Globals["IDInit"]))
    print("IDFin = " + str(Globals["IDFin"]))
    print("-----------------------------------------------------")

def TypeUpload(content,hashkey,Globals):

    hashPart  = hashlib.new("sha1",content)
    if (hashkey != hashPart.hexdigest() ) :
        return [b"0",b"Error de integridad"]
    else:

        if Checkintervale(Globals["IDInit"],Globals["IDFin"],int(hashkey, 16)) :
            path =  Globals["PathFile"] +"/"+ hashkey + ".rf"
            archivo = open(path,'ab')

            archivo.write(content)
            archivo.close()
            return [b"1",b"1",Strencode(hashkey)]
        else:
            return [b"1",b"2",Strencode(Globals["AntecesorIP"]),Strencode(Globals["AntecesorPort"])]

def TypeDowload(hashkey,Globals):

    if Checkintervale(Globals["IDInit"],Globals["IDFin"],int(hashkey, 16)) :
        FilePath =  Globals["PathFile"] +"/"+ hashkey + ".rf"
        file = open(FilePath, "rb")
        content = file.read()
        MSJ = [b"1",b"1",content]
        file.close()
        return MSJ

    else:
        return [b"1",b"2",Strencode(Globals["AntecesorIP"]),Strencode(Globals["AntecesorPort"])]
    
def TypeGiveFiles(IDini,IDFin,Globals):

    entries = os.listdir(Globals["PathFile"])
    for entry in entries:
        
        if ".rf" in entry : 
    
            HashFile = entry.split(".")
            HashFile = HashFile[0]
            if Checkintervale(IDini,IDFin,int(HashFile, 16)):
                file = open(entry, "rb")
                content = file.read()
                MSJ = [b"2",content,Strencode(entry)]
                file.close()
                os.remove(entry)
                return MSJ
    
    return [b"1"]

def Init():
    
        Globals = {}
        
        Globals["AntecesorIP"] = ""
        
        Globals["PathFile"] = os.path.dirname(os.path.abspath(__file__))
        
        Globals["context"] = zmq.Context()
        Globals["socketEscucha"] = Globals["context"].socket(zmq.REP)
        
        ip_puerto(Globals)

        Globals["socketEscucha"].bind("tcp://*:"+Globals["MyPort"])
        
        getID(Globals)
        
        
        if FirstConect(Globals) :

            while True:

                MSJData = Globals["socketEscucha"].recv_multipart()

                Type = Bdecode(MSJData[0])

                if Type == "0" :

                    Respt = CheckNewID(Bdecode(MSJData[1]),Bdecode(MSJData[2]),Bdecode(MSJData[3]),Globals)

                elif Type == "1" :
                     Respt = TypeUpload(MSJData[1],Bdecode(MSJData[2]),Globals)

                elif Type == "2" :
                     Respt = TypeDowload(Bdecode(MSJData[1]),Globals)
                
                elif Type == "3" :
                     Respt = TypeGiveFiles(Bdecode(MSJData[1]),Bdecode(MSJData[2]),Globals)


                Globals["socketEscucha"].send_multipart(Respt)

        else:
            print("END")


Init()
