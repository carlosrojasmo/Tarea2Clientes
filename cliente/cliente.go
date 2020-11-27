package main

import (
	"fmt"
	"context"
	"io"
	//"io/ioutil"
	"math"
	"os"
	"log"
	"time"
	//"strconv"
	"google.golang.org/grpc"
	pb "../proto"
	"log"
	"context"
	"time"
	"unsafe"
	
)

const (
	port = ":50051" //Quiza debamos usar distintos puertos segun en que trabajamos
	address = "10.10.28.10:50051"
	addressDataNode1  = "10.10.28.12:50051"
	addressDataNode2  = "10.10.28.13:50051"
)

func Chunking(name string) {

	fileToBeChunked :=name // change here!

	file, err := os.Open(fileToBeChunked)

	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 1 * (1 << 20) // 1 MB, change this to your requirement

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewLibroServiceClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := c.UploadBook(ctx)
	for i := uint64(0); i < totalPartsNum; i++ {

			partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)
			file.Read(partBuffer)

			stream.Send(&pb.SendChunk{Chunk : partBuffer,Offset : int64(i),Name : name})
					//aqui funcion de enviar
	}
	m, err := stream.CloseAndRecv()
	fmt.Println(m)

	}
func Unchunking(name string, name2 string){
	fileToBeChunked :=name // change here!
	//file, err := os.Open(fileToBeChunked)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
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

	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

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


	for  {
		newAdress, err := stream.Recv()
		if err == io.EOF {
        stream.SendAndClose(&pb.ReplyEmpty{Ok : "Ok"})
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
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

			// calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant

		var chunkSize int64 = int64(chunkInfo)
		chunkBufferBytes := make([]byte, chunkSize)

		fmt.Println("Appending at position : [", writePosition, "] bytes")
		writePosition = writePosition + chunkSize

			// read into chunkBufferBytes me tincA que se puede saltar y agregar directo
		

			// DON't USE ioutil.WriteFile -- it will overwrite the previous bytes!
			// write/save buffer to disk
			//ioutil.WriteFile(newFileName, chunkBufferBytes, os.ModeAppend)

		n, err := file.Write(chunkBufferBytes)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() //flush to disk

			// free up the buffer for next cycle
			// should not be a problem if the chunk size is small, but
			// can be resource hogging if the chunk size is huge.
			// also a good practice to clean up your own plate after eating

		chunkBufferBytes = nil // reset or empty our buffer

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

    	Unchunking(input2,input2 + "D")
    	
    } else {
    	fmt.Print("Ingrese la direccion  del libro que desea cargar : ")//se pide el nombre del libro
    	input2 , _ := reader.ReadString('\n')
    	input2 = strings.Replace(input2, "\n", "", -1)
    	input2 = strings.Replace(input2, "\r", "", -1)
    	Chunking(input2)
    }
    
	
}

