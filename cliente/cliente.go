package main

import (
	"fmt"
	"context"
	"io"
	//"io/ioutil"
	"math/rand"
	"math"
	"os"
	"log"
	"time"
	//"strconv"
	"google.golang.org/grpc"
	pb "../proto"
	"unsafe"
	"bufio"
	"strings"
	"io/ioutil"
	
)

const (
	nameNode = "10.10.28.10:50051"
	addressDataNode1 = "10.10.28.101:50051"
	addressDataNode2  = "10.10.28.100:50051"
	addressDataNode3  = "10.10.28.102:50051"
)

var dataNodes = [3]string{addressDataNode1,addressDataNode2,addressDataNode3}

func chunking(name string) {

	fileToBeChunked :=name // change here!

	file, err := os.Open(fileToBeChunked)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 250 * (1 << 10) // 1 MB, change this to your requirement

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	s1 := rand.NewSource(time.Now().UnixNano())
    r1 := rand.New(s1)
    ad := r1.Intn(len(dataNodes))
	fmt.Println("conectando...")
	conn, err := grpc.Dial(dataNodes[ad], grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
	for err != nil {
		fmt.Println("Fallo la conexion ,Intentando de nuevo")
		ad = r1.Intn(len(dataNodes))
		fmt.Println("conectando...")
		conn, err = grpc.Dial(dataNodes[ad], grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
	}
	defer conn.Close()
	fmt.Println("conectado!")

	c := pb.NewLibroServiceClient(conn)
    
	stream, err := c.UploadBook(context.Background()) //Empezamos a subir los chunks del libro
	for i := uint64(0); i < totalPartsNum; i++ {
			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)
			file.Read(partBuffer)

			stream.Send(&pb.SendChunk{Chunk : partBuffer,Offset : int64(i),Name : name})
					

	}
	_, err = stream.CloseAndRecv()
	

	}

func buscarLibro(name string) {
	
}

func unchunking(name string, name2 string){
	fileToBeChunked :=name 
			
	conn, err := grpc.Dial(nameNode, grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewLibroServiceClient(conn)
    
	newFileName := name2
	_, err = os.Create(newFileName)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}



	file, err := os.Create(newFileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	

	stream,err := c.GetAddressChunks(context.Background(),&pb.BookName{Name : fileToBeChunked})

	var writePosition int64 = 0

    j := 0
	for  {//Comenzamos a descargar los chunks y a juntarlos
		j++
		newAddress, err := stream.Recv()
		if err == io.EOF {
			break
    	}else if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		conn2, err := grpc.Dial(newAddress.GetUbicacion(), grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn2.Close()

		c2 := pb.NewLibroServiceClient(conn2)


		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	    defer cancel()

		newFileChunk, err := c2.DownloadChunk(ctx , &pb.ChunkId{Id : newAddress.GetId()})

		chunkInfo := unsafe.Sizeof(newFileChunk.GetChunk())
		fmt.Println("Chunkinfo: "+fmt.Sprint(chunkInfo))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

			
		
		var chunkSize int64 = int64(chunkInfo)
		
		fmt.Println("ChunkSize: "+fmt.Sprint(chunkSize))
		fmt.Println("Appending at position : [", writePosition, "] bytes")
		writePosition = writePosition + chunkSize


		n, err := file.Write(newFileChunk.GetChunk())

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() 

		fmt.Println("Written ", n, " bytes")

		fmt.Println("Recombining part [", j, "] into : ", newFileName)
	}

	// now, we close the newFileName
	file.Close()
}


func main(){

	reader := bufio.NewReader(os.Stdin)
    fmt.Println("Cliente")
    fmt.Println("---------------------")

    
    fmt.Print("Indique si desea hacer una descarga o una carga (D o U) : ")//se pide si es Downloader y Uploader
    input1, _ := reader.ReadString('\n')
    input1 = strings.Replace(input1, "\n", "", -1)
    input1 = strings.Replace(input1, "\r", "", -1)
    if input1 == "D" {
    	fmt.Print("Ingrese el nombre del libro : ")//se pide el nombre del libro
    	input2 , _ := reader.ReadString('\n')
    	input2 = strings.Replace(input2, "\n", "", -1)
    	input2 = strings.Replace(input2, "\r", "", -1)

    	unchunking(input2,strings.Split(input2,".")[0] + "D"+"."+strings.Split(input2,".")[1])
    	
    } else {
		files, err := ioutil.ReadDir("./")
    	if err != nil {
        	log.Fatal(err)
    	}

    	for _, f := range files {
			if f.Name() != "cliente.go"{
				fmt.Println(f.Name())
			}
            
    	}
    	fmt.Print("Ingrese la direccion  del libro que desea cargar : ")//se pide el nombre del libro
    	input2 , _ := reader.ReadString('\n')
    	input2 = strings.Replace(input2, "\n", "", -1)
    	input2 = strings.Replace(input2, "\r", "", -1)
    	chunking(input2)
    }
    
	
}

