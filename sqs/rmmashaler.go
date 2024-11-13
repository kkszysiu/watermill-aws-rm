package sqs

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bytedance/sonic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/ThreeDotsLabs/watermill/message"
)

type rmMongoSqsMessage struct {
	ID string `json:"_id"`
}

type RmMongoMarshaler interface {
	Marshal(msg *message.Message) (*types.Message, error)
}

type RmMongoUnmarshaler interface {
	Unmarshal(msg *types.Message) (*message.Message, error)
}

type DefaultRmMongoMarshalerUnmarshaler struct {
	MongoCollection *mongo.Collection
}

func (d DefaultRmMongoMarshalerUnmarshaler) Marshal(msg *message.Message) (*types.Message, error) {
	attributes := metadataToAttributes(msg.Metadata)
	// client side uuid
	// there is a deduplication id that can be use for
	// fifo queues
	attributes[UUIDAttribute] = types.MessageAttributeValue{
		StringValue: aws.String(msg.UUID),
		DataType:    aws.String(AWSStringDataType),
	}
	// Insert the message into the MongoDB
	insertedResult, err := d.MongoCollection.InsertOne(context.Background(), msg.Payload)
	if err != nil {
		return nil, err
	}

	if insertedResult.InsertedID == nil {
		return nil, errors.New("inserted ID is nil")
	}
	if insertedResult.InsertedID == primitive.NilObjectID {
		return nil, errors.New("inserted ID is nil object ID")
	}

	return &types.Message{
		MessageAttributes: attributes,
		Body:              aws.String(insertedResult.InsertedID.(primitive.ObjectID).String()),
	}, nil
}

func (d DefaultRmMongoMarshalerUnmarshaler) Unmarshal(msg *types.Message) (*message.Message, error) {
	var uuid, payload string
	attributes := attributesToMetadata(msg.MessageAttributes)
	if value, ok := msg.MessageAttributes[UUIDAttribute]; ok {
		uuid = *value.StringValue
		delete(attributes, UUIDAttribute)
	}

	if msg.Body != nil {
		payload = *msg.Body
	}

	rmSqsMessage := rmMongoSqsMessage{}

	err := sonic.Unmarshal([]byte(payload), &rmSqsMessage)
	if err != nil {
		return nil, err
	}

	// Fetch data from MongoDB
	objectId, err := primitive.ObjectIDFromHex(rmSqsMessage.ID)
	if err != nil {
		return nil, err
	}
	raw, err := d.MongoCollection.FindOne(context.Background(), bson.M{"_id": objectId}).Raw()
	if err != nil {
		return nil, err
	}

	wmsg := message.NewMessage(uuid, message.Payload(raw))
	wmsg.Metadata = attributes

	return wmsg, nil
}
