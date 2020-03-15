import zmq
import random
import hashlib
import os

IDInit = 0
IDFin = 0

MyIP = ""
MyPort = ""

ServerID = ""

AntecesorIP = ""
AntecesorPort = ""

context = zmq.Context()
socketEscucha = context.socket(zmq.REP)

PathFile = os.path.dirname(os.path.abspath(__file__))

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")


def ip_puerto():
        global MyIP
        MyIP = input("INGRESE LA IP del server\n")
        MyIP = "localhost"
        global MyPort
        MyPort = input("INGRESE PUERTO del server\n")

ip_puerto()

socketEscucha.bind("tcp://*:"+MyPort)

def SendSocketMSJ(IpServer,PortServer,MSJ):
    global context
    Path = "tcp://"+IpServer+":"+PortServer
    socket = context.socket(zmq.REQ)
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

def inToAnillo():

    global ServerID
    global MyIP
    global MyPort
    global IDFin
    global IDInit
    global AntecesorIP
    global AntecesorPort

    FirstIP = input("INGRESE LA IP DE ALGUN NODO\n")
    FirstIP = "localhost"
    FirstPort = input("INGRESE EL PUERTO DEL NODO\n")

    while True:
        MSJData = SendSocketMSJ(FirstIP,FirstPort,[b'0',Strencode(ServerID),Strencode(MyIP),Strencode(MyPort)])
        if (Bdecode(MSJData[0]) != "1"):
            print("Error conectandose con el nodo")
            return 0
        else:
            if (Bdecode(MSJData[1]) == "1"):
                IDInit =   Bdecode(MSJData[2])
                AntecesorIP = Bdecode(MSJData[3])
                AntecesorPort = Bdecode(MSJData[4])
                break
            else:

                FirstIP = Bdecode(MSJData[2])
                FirstPort = Bdecode(MSJData[3])

    IDFin = ServerID
    PrintMyRange()

def FirstConect():

    global IDInit
    global IDFin
    global ServerID


    if not CheckFirstNode():

        inToAnillo()

    else:

        if int(ServerID) == 2**160 :
            IDInit = 0
        else:
            IDInit = int(ServerID)+1

        IDFin = ServerID

    return 1

def Checkintervale(inicio,Fin,Value):

    if int(inicio) > int(Fin) :
        return (int(Value) <= int(Fin)  and int(Value) >= 0) or ( int(Value) <= 2**160 and int(Value) >= int(inicio))
    else:
        return (int(Value) >= int(inicio)  and int(Value) <= int(Fin))


def getID():

    global ServerID
    Random = random.randint(0,100) #25
    ServerID = Random
    hash  = hashlib.new("sha1",Strencode(Random))
    ServerID = int(hash.hexdigest(), 16)
    print("ServerID: "+str(ServerID))

def CheckNewID(NewServerID,IP,PORT):

    global IDInit
    global IDFin

    global AntecesorIP
    global AntecesorPort

    global MyIP
    global MyPort

    global ServerID

    if Checkintervale(IDInit,IDFin,NewServerID) :

        IdInicopy = IDInit

        if int(NewServerID) == 2**160 :
            IDInit = 0
        else:
            IDInit = int(NewServerID)+1

        if AntecesorIP ==  "":
             respt = [b"1",b"1",Strencode(IdInicopy),Strencode(MyIP),Strencode(MyPort)]

        else:
             respt = [b"1",b"1",Strencode(IdInicopy),Strencode(AntecesorIP),Strencode(AntecesorPort)]

        AntecesorIP    = IP
        AntecesorPort  = PORT
        PrintMyRange()
        return respt
    else:
        return [b"1",b"2",Strencode(AntecesorIP),Strencode(AntecesorPort)]

def PrintMyRange():

    global IDInit
    global IDFin
    global AntecesorPort

    print("-----------------------------------------------------")
    print("IDIni = " + str(IDInit))
    print("IDFin = " + str(IDFin))
    print("-----------------------------------------------------")

def TypeUpload(content,hashkey):

    global PathFile
    global IDInit
    global IDFin
    global AntecesorIP
    global AntecesorPort

    hashPart  = hashlib.new("sha1",content)
    if (hashkey != hashPart.hexdigest() ) :
        return [b"0",b"Error de integridad"]
    else:

        if Checkintervale(IDInit,IDFin,int(hashkey, 16)) :
            path =  PathFile +"/"+ hashkey + ".rf"
            archivo = open(path,'ab')

            archivo.write(content)
            archivo.close()
            return [b"1",b"1",Strencode(hashkey)]
        else:
            return [b"1",b"2",Strencode(AntecesorIP),Strencode(AntecesorPort)]

def TypeDowload(hashkey):

    if Checkintervale(IDInit,IDFin,int(hashkey, 16)) :
        FilePath =  PathFile +"/"+ hashkey + ".rf"
        file = open(FilePath, "rb")
        content = file.read()
        MSJ = [b"1",b"1",content]
        file.close()
        return MSJ

    else:
        return [b"1",b"2",Strencode(AntecesorIP),Strencode(AntecesorPort)]

def Init():

        global socketEscucha
        getID()
        if FirstConect() :

            while True:

                MSJData = socketEscucha.recv_multipart()

                Type = Bdecode(MSJData[0])

                if Type == "0" :

                    Respt = CheckNewID(Bdecode(MSJData[1]),Bdecode(MSJData[2]),Bdecode(MSJData[3]))

                elif Type == "1" :
                     Respt = TypeUpload(MSJData[1],Bdecode(MSJData[2]))

                elif Type == "2" :
                     Respt = TypeDowload(Bdecode(MSJData[1]))


                socketEscucha.send_multipart(Respt)

        else:
            print("END")


Init()
