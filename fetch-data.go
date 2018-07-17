package main

import (
	"fmt"
	"os"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

type dynamoDB_data struct {
	Timestamp      int64  `dynamodbav:"tis"`
	Variable_A  int64 `dynamodbav:"Var_A"`
	Variable_B  int64 `dynamodbav:"Var_B"`
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	InitialStart := 1531161000
	API_Analytics_Run(int64(InitialStart))

}

func API_Analytics_Run(s_tis int64) {

	data_main := []dynamoDB_data{}
	data := []dynamoDB_data{}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewSharedCredentials("aws.creds", "default"),
	})
	if err != nil {
		exitWithError(fmt.Errorf("failed to make Query API call, %v", err))
	}
	svc := dynamodb.New(sess)

	// Build the query input parameters
	queryInput := &dynamodb.QueryInput{}
	queryInput = &dynamodb.QueryInput{
		//Limit: aws.Int64(10),
		TableName: aws.String("facade_api_log"),
		ExpressionAttributeNames: map[string]*string{"#s": aws.String("status"), },
		ScanIndexForward: aws.Bool(true),
		ProjectionExpression: aws.String("tis,Var_A,Var_B,#s,ip"),

		KeyConditions: map[string]*dynamodb.Condition{
			"env": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList:     []*dynamodb.AttributeValue{
					{
						N: aws.String("1"),
					},
				},
			},
			"tis": {
				ComparisonOperator: aws.String("BETWEEN"),
				AttributeValueList:     []*dynamodb.AttributeValue{
					{
						N: aws.String(fmt.Sprintf("%d", (s_tis * 1000000000))),
					},
					{
						N: aws.String(fmt.Sprintf("%d", (s_tis + 86400) * 1000000000)),
					},
				},
			},

		},
	}
	data_pending:
	data = nil
	var resp1, err1 = svc.Query(queryInput)
	if err1 != nil {
		fmt.Println(err1)
	} else {

		//log.Println(resp1.Items)
		err = dynamodbattribute.UnmarshalListOfMaps(resp1.Items, &data)
		if err != nil {
			exitWithError(fmt.Errorf("failed to unmarshal Query result items, %v", err))
		}

		//println(len(data))

	}
	if (resp1.LastEvaluatedKey == nil ) {
		//println("No more data pending")
		for i := 0; i < len(data); i++ {
			data_main = append(data_main, data[i])
		}
	} else {
		//println("More Data Pending")
		for i := 0; i < len(data); i++ {
			data_main = append(data_main, data[i])
		}
		queryInput.ExclusiveStartKey = resp1.LastEvaluatedKey
		goto data_pending
	}

	fmt.Printf("%d",len(data_main))

}
