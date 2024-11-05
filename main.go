package earedis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/eris-apple/eactx"
	"github.com/eris-apple/ealogger"
	rdb "github.com/redis/go-redis/v9"
	"reflect"
	"time"
)

type Client = rdb.Client

// ConnectConfig — the structure for connecting to redis.
type ConnectConfig struct {
	Addr              string
	User              string
	Password          string
	DB                int
	pingConnectionTTL *time.Duration
}

// Service — redis service.
type Service struct {
	l *ealogger.Logger
	c *ConnectConfig

	client *Client

	traceName string
}

// Init — initializing the connection with redis.
func (s *Service) Init() error {
	s.client = rdb.NewClient(&rdb.Options{
		Addr:     s.c.Addr,
		Username: s.c.User,
		Password: s.c.Password,
		DB:       s.c.DB,
	})

	ctx := eactx.NewContextWithTimeout(context.Background(), *s.c.pingConnectionTTL)
	if err := s.client.Ping(ctx.GetContext()).Err(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to connect to redis", err)
		return err
	}

	s.l.InfoT(s.traceName, "Successfully connected to redis")
	return nil
}

// Disconnect — disconnecting from redis.
func (s *Service) Disconnect() error {
	if err := s.client.Close(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to disconnect from redis", err)
		return err
	}

	s.l.InfoT(s.traceName, "Successfully disconnected to redis")
	s.client = nil
	return nil
}

func (s *Service) Set(ctx *eactx.Context, key string, value interface{}, expiration time.Duration) error {
	if err := s.client.Set(ctx.GetContext(), key, value, expiration).Err(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to set key", key, err)
		return err
	}

	return nil
}

func (s *Service) SAdd(ctx *eactx.Context, key string, members ...interface{}) error {
	if err := s.client.SAdd(ctx.GetContext(), key, members).Err(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to set members at key", key, err)
		return err
	}

	return nil
}

func (s *Service) SMembers(ctx *eactx.Context, key string) ([]string, error) {
	result, err := s.client.SMembers(ctx.GetContext(), key).Result()
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to set members at key", key, err)
		return nil, err
	}

	return result, nil
}

func (s *Service) SMembersWithChild(ctx *eactx.Context, key string) ([]string, error) {
	s.l.InfoT(s.traceName, "Get members child by key ", key)
	members, err := s.client.SMembers(ctx.GetContext(), key).Result()
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to set members at key", key, err)
		return nil, err
	}

	result := make([]string, 0)
	for _, member := range members {
		v, err := s.Get(ctx, member)
		if err != nil || len(v) == 0 {
			s.l.ErrorT(s.traceName, "Failed to get member", member, err)
			continue
		}

		result = append(result, v)
	}

	return result, nil
}

func (s *Service) JSONSMembersWithChild(ctx *eactx.Context, key string, v interface{}) error {
	result, err := s.SMembersWithChild(ctx, key)
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to set members at key", key, err)
		return err
	}

	sliceValue := reflect.ValueOf(v).Elem()
	elemType := sliceValue.Type().Elem()

	for _, item := range result {
		newElem := reflect.New(elemType).Elem()

		err := json.Unmarshal([]byte(item), newElem.Addr().Interface())
		if err != nil {
			return err
		}

		sliceValue.Set(reflect.Append(sliceValue, newElem))
	}

	return nil
}

func (s *Service) Get(ctx *eactx.Context, key string) (string, error) {
	result, err := s.client.Get(ctx.GetContext(), key).Result()
	if err != nil || len(result) == 0 {
		s.l.ErrorT(s.traceName, "Failed to get key", key, err)
		return "", err
	}

	return result, nil
}

func (s *Service) JSONGet(ctx *eactx.Context, key string, v interface{}) error {
	result, err := s.client.Get(ctx.GetContext(), key).Result()
	if err != nil || len(result) == 0 {
		s.l.ErrorT(s.traceName, "Failed to get key", key, err)
		return err
	}

	if err := json.Unmarshal([]byte(result), v); err != nil {
		return err
	}

	return nil
}

func (s *Service) MGet(ctx *eactx.Context, key ...string) ([]interface{}, error) {
	result, err := s.client.MGet(ctx.GetContext(), key...).Result()
	if err != nil {
		s.l.ErrorT(s.traceName, "Failed to get key", key, err)
		return nil, err
	}

	return result, nil
}

func (s *Service) Del(ctx *eactx.Context, keys ...string) error {
	if err := s.client.Del(ctx.GetContext(), keys...).Err(); err != nil {
		s.l.ErrorT(s.traceName, "Failed to delete keys", keys, err)
		return err
	}

	return nil
}

// NewService — returns the Service instance.
func NewService(l *ealogger.Logger, c *ConnectConfig, traceName string) *Service {
	if c.pingConnectionTTL == nil {
		defaultPingConnectionTTL := 30 * time.Second
		c.pingConnectionTTL = &defaultPingConnectionTTL
	}

	return &Service{
		l: l,
		c: c,

		traceName: fmt.Sprintf("[%s_RedisService]", traceName),
	}
}
