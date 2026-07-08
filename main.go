package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Form struct {
    Name        string `json:"name"`
    Email       string `json:"email"`
    Interest    string `json:"interest"`
    Description string `json:"description"`
    WebsiteURL  string `json:"website_url"`
}

type RateLimiter struct {
	mutex sync.Mutex 
	ip_adresses map[string]int 
	limit int
}

type EmailJob struct {
    Subject string
    Body    string
}

// public variables
var SMTP_LOGIN string = os.Getenv("SMTP_LOGIN")
var SMTP_PASSWORD string = os.Getenv("SMTP_PASSWORD")
var SMTP_PORT = os.Getenv("SMTP_PORT")
var SMTP_HOST = os.Getenv("SMTP_HOST")
var EMAIL_DESTINATION string = os.Getenv("EMAIL_DESTINATION")
var ALLOWED_ORIGINS string = os.Getenv("ALLOWED_ORIGINS")
var ALLOWED_ORIGINS_MAP map[string]bool = make(map[string]bool)

var FORM Form

var emailQueue chan EmailJob = make(chan EmailJob, 100)

func StartEmailWorker() {
	go func() {
		var smtpAddress string  = SMTP_HOST + ":" + SMTP_PORT
		var auth smtp.Auth = smtp.PlainAuth("", SMTP_LOGIN, SMTP_PASSWORD, SMTP_HOST)
		var to []string = []string{EMAIL_DESTINATION}

		for {
            var job EmailJob = <-emailQueue

            var messageString string = "To: " + EMAIL_DESTINATION + "\r\n" +
                "Subject: " + job.Subject + "\r\n" +
                "Content-Type: text/plain; charset=UTF-8\r\n" +
                "\r\n" + 
                job.Body


			fmt.Println("\n==========================================")
            fmt.Println("🚀 WORKER ZDJĄŁ ZADANIE Z KOLEJKI:")
            fmt.Println(messageString)
            fmt.Println("==========================================")

            var messageBytes []byte = []byte(messageString)
            var sendErr error = smtp.SendMail(smtpAddress, auth, SMTP_LOGIN, to, messageBytes)
            if sendErr != nil {
                fmt.Println("CRITICAL: Nie udało się wysłać maila z kolejki. Błąd:", sendErr)
            } else {
                fmt.Println("SUCCESS: Mail z kolejki został wysłany.")
            }
		}
	}()
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	var rateLimiter *RateLimiter = &RateLimiter{
		ip_adresses: make(map[string]int),
		limit: limit,
	}

	go func() {
		var ticker *time.Ticker = time.NewTicker(window)

		for range ticker.C {
			rateLimiter.mutex.Lock()
			rateLimiter.ip_adresses = make(map[string]int)
			rateLimiter.mutex.Unlock()
		}
	}()

	return rateLimiter
}

func (rateLimiter *RateLimiter) Allow(ip string) bool {
	rateLimiter.mutex.Lock()

	defer rateLimiter.mutex.Unlock()

	var currentCount int = rateLimiter.ip_adresses[ip]
	currentCount++
	rateLimiter.ip_adresses[ip] = currentCount

	if currentCount > rateLimiter.limit {
		return false
	}

	return true
}

func clientIP(read *http.Request) string{
		var clientIPAddress string = read.Header.Get("X-Forwarded-For");

		if clientIPAddress != "" {
			var ips []string  = strings.Split(clientIPAddress, ",")
			clientIPAddress = strings.TrimSpace(ips[0])
		}
		if clientIPAddress == "" {
			clientIPAddress = read.Header.Get("X-Real-IP")
		}
		if clientIPAddress == "" {
			ip, _, err := net.SplitHostPort(read.RemoteAddr)
			if err != nil {
				clientIPAddress = read.RemoteAddr
			} else {
				clientIPAddress = ip
			}
		}
		return clientIPAddress
} 

func init() {
	if ALLOWED_ORIGINS == "" {
        fmt.Println("Error, lack of allowed origins")
        return
    }
	var originsList []string = strings.Split(ALLOWED_ORIGINS, ",")

	for _, origin := range originsList {
        var cleanOrigin string = strings.TrimSpace(origin)
        ALLOWED_ORIGINS_MAP[cleanOrigin] = true
    }
}

func main()  {
	//LIMITER(CHANCES, MINUTES)
	var limiter *RateLimiter = NewRateLimiter(3, 1*time.Minute)

	StartEmailWorker()

	http.HandleFunc("/health", func(writer http.ResponseWriter, read *http.Request) {
        writer.WriteHeader(http.StatusOK)
        writer.Write([]byte("OK"))
    })
	
	http.HandleFunc("/api/proxy", func (writer http.ResponseWriter, read *http.Request) {
		
		//PREFLIGHT ORIGIN CHECK
		var origin string = read.Header.Get("Origin")
		if !ALLOWED_ORIGINS_MAP[origin] {
			http.Error(writer, "Brak dostępu CORS", http.StatusForbidden)
			return
		}

		// CORS HEADERS
		writer.Header().Set("Access-Control-Allow-Origin", origin)
		writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		writer.Header().Set("Access-Control-Max-Age", "86400")


		// PREFLIGHT ANSWER
		if read.Method == "OPTIONS" {
			writer.WriteHeader(http.StatusOK)
			return
		}


		//CLIENT IP
		var clientIPAddress string  = clientIP(read)
		if clientIPAddress == "" {
            http.Error(writer, "Lack of IP address.", http.StatusBadRequest)
            return
        }

		var isAllowed bool = limiter.Allow(clientIPAddress)

		if isAllowed == false {
			http.Error(writer, "Too many requests.", http.StatusTooManyRequests)
			return
		}

		// JSON PAYLOAD LIMIT 
		var maxBytes int64 = 1024 * 1024
		read.Body = http.MaxBytesReader(writer, read.Body, maxBytes)

		var formData Form

		var decoder *json.Decoder = json.NewDecoder(read.Body)

		var err error = decoder.Decode(&formData)

		if err != nil {
            http.Error(writer, "Error, payload too large", http.StatusRequestEntityTooLarge) // 413 Payload Too Large
            return
		}

		// DATA VALIDATION
		if formData.WebsiteURL != "" {
            writer.WriteHeader(http.StatusOK)
            return
        }

		formData.Email = strings.TrimSpace(formData.Email)
        formData.Interest = strings.TrimSpace(formData.Interest)
        formData.Description = strings.TrimSpace(formData.Description)
        formData.Name = strings.TrimSpace(formData.Name)

		if formData.Email == "" || formData.Name == "" || formData.Description == "" || formData.Interest == "" {
			http.Error(writer, "Bad Request", http.StatusBadRequest)
     		return
        }

		var emailErr error
		_, emailErr = mail.ParseAddress(formData.Email)
        if emailErr != nil {
            http.Error(writer, "Invalid Data", http.StatusBadRequest)
            return
        }

		var nameLength int = utf8.RuneCountInString(formData.Name)
		if nameLength < 5 || nameLength > 150 {
            http.Error(writer, "Invalid Data", http.StatusBadRequest)
    		return
		}

		var descriptionLength int = utf8.RuneCountInString(formData.Description)
		if descriptionLength < 30 || descriptionLength > 5000 {
            http.Error(writer, "Invalid Data", http.StatusBadRequest)
    		return
		}
		

		var subject string = "INOGI - Nowe zapytanie: " + formData.Interest
        
        var body string = "Otrzymałeś nową wiadomość z formularza kontaktowego.\n\n" +
                          "Od: " + formData.Name + " (" + formData.Email + ")\n" +
                          "Temat/Kategoria: " + formData.Interest + "\n\n" +
                          "Treść wiadomości:\n" + formData.Description

        var newJob EmailJob = EmailJob{
            Subject: subject,
            Body:    body,
        }

		emailQueue <- newJob
		
		writer.WriteHeader(http.StatusOK)
        writer.Write([]byte("Wiadomość została wysłana pomyślnie!"))

	})
	http.ListenAndServe(":8080", nil)
}
