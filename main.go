package main

import (
	"log"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	tableName = "DUALIS_CREDENTIALS"
)

type User struct {
	Email             string `json:"email"`
	Password          string `json:"password"`
	NotificationEmail string `json:"notificationEmail"`
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

	for _, userMap := range result.Items {
		user := User{}
		err = dynamodbattribute.UnmarshalMap(userMap, &user)
		if err != nil {
			log.Fatalf("Got error unmarshalling: %s", err)
			return err
		}
		waitGroup := sync.WaitGroup{}
		go func(user User) {
			waitGroup.Add(1)

		}(user)
	}
	//TODO invoke other function

	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
