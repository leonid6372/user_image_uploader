package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	minioServer "main/minio"
	"os"

	_ "github.com/lib/pq"
)

const (
	url    = "0.0.0.0:1234"
	bucket = "bucketName"

	qrGetUsername  = `SELECT username FROM "user" WHERE full_name = $1;`
	qrNewUserImage = `UPDATE "user" SET image_path = $1 WHERE username = $2;`
)

func main() {
	fmt.Println("users photo uploading started...")

	DB, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=somePass dbname=someName sslmode=disable") // Подключаемся к БД
	if err != nil {
		panic(err)
	}
	defer DB.Close()

	// Создание и подключение MinIO сервера
	miniosrv, err := minioServer.New(url, bucket, false)
	if err != nil {
		panic(err)
	}
	err = miniosrv.Connect()
	if err != nil {
		panic(err)
	}

	// Читаем файлы из папки photo
	files, err := os.ReadDir("photo")
	if err != nil {
		panic(err)
	}

	// Обрабатываем каждое фото по очереди
	for _, file := range files {
		fileName := file.Name()

		var fullName []string // 0. secondName, 1. firstName, 2. patronymic
		var extension string

		// Парсим имя фото
		{
			var namePart string
			for i, rn := range fileName {
				if rn == '_' {
					fullName = append(fullName, namePart)
					namePart = ""
					continue
				}
				if rn == '.' {
					fullName = append(fullName, namePart)
					extension = fileName[i+1:]
					break
				}
				namePart += string(rn)
			}
		}
		if len(fullName) < 3 {
			fmt.Println("photo", fileName, "has problem. wrong file name")
		}
		fullNameStr := fullName[0] + " " + fullName[1] + " " + fullName[2]

		// Читаем фото
		var f *os.File
		{
			f, err = os.Open(fmt.Sprintf("photo/%s", fileName))
			if err != nil {
				fmt.Println("photo", fileName, "has problem")
				panic(err)
			}
		}
		defer f.Close()

		// Узнаем username по ФИО из БД
		var username string
		err := DB.QueryRow(qrGetUsername, fullNameStr).Scan(&username)
		// Если не нашли ФИО в БД, то пропускаем
		if err == sql.ErrNoRows {
			fmt.Println("photo", fileName, "has problem. didn't found full_name in DB")
			continue
		}
		if err != nil {
			fmt.Println("photo", fileName, "has problem")
			panic(err)
		}

		// Готовим информацию о фото для загрузки в minio
		minioName := fmt.Sprintf("user_images/%s", username)
		fileInfo, err := f.Stat()
		if err != nil {
			fmt.Println("photo", fileName, "has problem")
			panic(err)
		}
		fileSize := fileInfo.Size()

		// Загружаем фото пользователя в minio
		{
			image := minioServer.Image{
				Payload:   io.Reader(f),
				Name:      minioName,
				Size:      fileSize,
				Extension: extension,
			}

			// Отправляем фото в хранилище
			err = miniosrv.UploadImage(context.Background(), image)
			if err != nil {
				fmt.Println("photo", fileName, "has problem")
				panic(err)
			}
		}

		// Добавляем путь к фото в БД
		_, err = DB.Exec(qrNewUserImage, fmt.Sprintf("https://images-here.ru/image?name=%s", minioName), username)
		if err != nil {
			fmt.Println("photo", fileName, "has problem")
			panic(err)
		}

		fmt.Println("photo", fileName, "successfully uploaded")
	}

	fmt.Println("users photo uploading successfully finished!")
}
