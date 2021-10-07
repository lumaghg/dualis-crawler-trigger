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
		go func() {
			defer wg.Done()

			user := User{}
			err = dynamodbattribute.UnmarshalMap(userMap, &user)
			if err != nil {
				log.Fatalf("Got error unmarshalling: %s", err)
				errChan <- err
			}

			payload, err := json.Marshal(user)
			if err != nil {
				log.Fatal(err)
				errChan <- err
			}

			_, err = lambdaClient.Invoke(&lambdaService.InvokeInput{FunctionName: aws.String("triggerScrapeGradesAndNotify"), Payload: payload})
			if err != nil {
				fmt.Println("Error calling MyGetItemsFunction")
				errChan <- err
			}
		}()

	}

	wg.Wait()
	if len(errChan) > 0 {
		return err
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
