package main

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Standard response format
type Response struct {
	Status  int         `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Helper function to send a JSON response
func SendResponse(c *gin.Context, status int, message string, data interface{}) {
	c.JSON(status, Response{
		Status:  status,
		Message: message,
		Data:    data,
	})
}

func OkResponse(c *gin.Context, status int, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Status:  status,
		Message: message,
		Data:    data,
	})
}
func OkRes(c *gin.Context, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Status:  http.StatusOK,
		Message: message,
		Data:    data,
	})
}

func ErrRes(c *gin.Context, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Status:  199,
		Message: message,
		Data:    data,
	})
}

func getJson(context *gin.Context) map[string]interface{} {
	b, _ := context.GetRawData()
	var reqBody map[string]interface{}

	json.Unmarshal(b, &reqBody)
	return reqBody
}

// Error handling middleware
func ErrorHandlingMiddleware() gin.HandlerFunc {
	// L()：获取全局logger
	logger := zap.L().Sugar()
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Log the error (you might want to log to a file or monitoring service)
				logger.Errorw("Recovered from panic: ", zap.Any("panic_info", err))

				// Send a unified error response
				SendResponse(c, http.StatusInternalServerError, "Internal Server Error", nil)
			}
		}()

		c.Next()

		// Check if there were any errors during request processing
		if len(c.Errors) > 0 {
			// Log the errors
			for _, e := range c.Errors {
				logger.Errorw("Error", zap.Error(e.Err))
			}

			// Determine the response status code based on error type
			var statusCode int
			if len(c.Errors) > 0 && c.Errors[0].Err == http.ErrHandlerTimeout {
				statusCode = http.StatusRequestTimeout
			} else {
				statusCode = http.StatusBadRequest
			}

			// Send a unified error response
			SendResponse(c, statusCode, "Bad Request", nil)
		}
	}
}
