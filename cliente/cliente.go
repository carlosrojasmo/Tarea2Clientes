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
	conn, err := grpc.Dial(dataNodes[ad], grpc.WithInsecure(), grpc.WithBlock())
	for err != nil {
		fmt.Println("Fallo la conexion ,Intentando de nuevo")
		ad = r1.Intn(len(dataNodes))
		fmt.Println("conectando...")
		conn, err = grpc.Dial(dataNodes[ad], grpc.WithInsecure(), grpc.WithBlock())
	}
	defer conn.Close()
	fmt.Println("conectado!")

	c := pb.NewLibroServiceClient(conn)
    //ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	stream, err := c.UploadBook(context.Background())
	for i := uint64(0); i < totalPartsNum; i++ {
			fmt.Println("principioo del for")
			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)
			file.Read(partBuffer)

			stream.Send(&pb.SendChunk{Chunk : partBuffer,Offset : int64(i),Name : name})
					//aqui funcion de enviar
			fmt.Println("estamos aqui")
	}
	m, err := stream.CloseAndRecv()
	fmt.Println(m)
	fmt.Println(err)

	}

func buscarLibro(name string) {
	
}

func unchunking(name string, name2 string){
	fileToBeChunked :=name // change here!
	//file, err := os.Open(fileToBeChunked)
			
	conn, err := grpc.Dial(nameNode, grpc.WithInsecure(), grpc.WithBlock())
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

	//set the newFileName file to APPEND MODE!!
	// open files r and w

	file, err := os.Create(newFileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// IMPORTANT! do not defer a file.Close when opening a file for APPEND mode!
	// defer file.Close()

	// just information on which part of the new file we are appending0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stream,err := c.GetAddressChunks(ctx,&pb.BookName{Name : fileToBeChunked})

	var writePosition int64 = 0

    j := 0
	for  {
		j++
		newAddress, err := stream.Recv()
		if err == io.EOF {
			break
    	}else if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		conn2, err := grpc.Dial(newAddress.GetUbicacion(), grpc.WithInsecure(), grpc.WithBlock())
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

			// calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant
		
		var chunkSize int64 = int64(chunkInfo)
		//chunkBufferBytes := make([]byte,chunkSize)
		// read into chunkBufferBytes
		fmt.Println("ChunkSize: "+fmt.Sprint(chunkSize))
		fmt.Println("Appending at position : [", writePosition, "] bytes")
		writePosition = writePosition + chunkSize

			// read into chunkBufferBytes me tincA que se puede saltar y agregar directo
		

			// DON't USE ioutil.WriteFile -- it will overwrite the previous bytes!
			// write/save buffer to disk
			//ioutil.WriteFile(newFileName, chunkBufferBytes, os.ModeAppend)

		n, err := file.Write(newFileChunk.GetChunk())

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() //flush to disk

			// free up the buffer for next cycle
			// should not be a problem if the chunk size is small, but
			// can be resource hogging if the chunk size is huge.
			// also a good practice to clean up your own plate after eating

		//chunkBufferBytes = nil // reset or empty our buffer

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

