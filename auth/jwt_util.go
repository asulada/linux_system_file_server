package auth

import (
	"fmt"
	"time"
)
import "github.com/golang-jwt/jwt/v5"

// 定义密钥（从配置文件读取）
var jwtSecret []byte

// SetJwtSecret 设置 JWT 密钥（在初始化时调用）
func SetJwtSecret(secret string) {
	jwtSecret = []byte(secret)
}

// GetJwtSecret 获取 JWT 密钥
func GetJwtSecret() []byte {
	return jwtSecret
}

// 自定义声明
type CustomClaims struct {
	UserID int `json:"user_id"`
	jwt.RegisteredClaims
}

// 生成JWT
func GenerateToken(userID int) (string, error) {
	// 创建声明
	claims := CustomClaims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)), // 过期时间
			IssuedAt:  jwt.NewNumericDate(time.Now()),                     // 签发时间
			Issuer:    "swagger.vip",                                      // 签发者
		},
	}

	// 创建token对象
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 使用密钥签名并生成token字符串
	return token.SignedString(jwtSecret)
}

// 验证和解析JWT
func ParseToken(tokenString string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// 确保签名方法正确
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	// 验证token
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	} else {
		return nil, fmt.Errorf("invalid token")
	}
}
