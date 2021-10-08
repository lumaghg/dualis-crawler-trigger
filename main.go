package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	lambdaService "github.com/aws/aws-sdk-go/service/lambda"
)

const (
	tableName = "DUALIS_CREDENTIALS"
)

type User struct {
	Email             string
	Password          string
	NotificationEmail string
}

func HandleRequest() error {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	dynamoClient := dynamodb.New(sess)
	lambdaClient := lambdaService.New(sess, &aws.Config{Region: aws.String("eu-central-1")})
	//get all the users and invoke the scrapeGradesAndNotify function
	proj := expression.NamesList(expression.Name("Email"), expression.Name("Password"), expression.Name("NotificationEmail"))

	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		log.Fatalf("Got error building expression: %s", err)
		return err
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	result, err := dynamoClient.Scan(params)
	if err != nil {
		log.Fatalf("Query API call failed: %s", err)
		return err
	}

	wg := sync.WaitGroup{}
	errChan := make(chan error, 1)
	for _, userMap := range result.Items {

		wg.Add(1)
		go func(errChan chan error) {
			//TODO function doesnt end
			defer wg.Done()
			defer fmt.Println("one done")
			fmt.Println("one start")
			user := User{}
			err = dynamodbattribute.UnmarshalMap(userMap, &user)
			fmt.Println(user)
			if err != nil {
				log.Fatalf("Got error unmarshalling: %s", err)
				errChan <- err
				return
			}

			payload, err := json.Marshal(user)
			if err != nil {
				log.Fatal(err)
				errChan <- err
				return
			}

			_, err = lambdaClient.Invoke(&lambdaService.InvokeInput{FunctionName: aws.String("triggerScrapeGradesAndNotify"), Payload: payload})
			if err != nil {
				fmt.Println("Error calling MyGetItemsFunction")
				errChan <- err
				return
			}
		}(errChan)

	}
	fmt.Println("start the wait")
	wg.Wait()
	fmt.Println("end the wait")
	if len(errChan) > 0 {
		return err
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
