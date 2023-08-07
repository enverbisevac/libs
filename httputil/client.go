// Copyright (c) 2023 Enver Bisevac
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package httputil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	client *http.Client
	base   string
	debug  bool
}

func NewClient(uri string, options ...ClientOption) *Client {
	c := &Client{
		client: http.DefaultClient,
		base:   uri,
		debug:  false,
	}

	for _, opt := range options {
		opt.Apply(c)
	}

	return c
}

// SetClient sets the default http client. This can be
// used in conjunction with golang.org/x/oauth2 to
// authenticate requests to the server.
func (c *Client) SetClient(client *http.Client) {
	c.client = client
}

// SetDebug sets the debug flag. When the debug flag is
// true, the http.Resposne body to stdout which can be
// helpful when debugging.
func (c *Client) SetDebug(debug bool) {
	c.debug = debug
}

// helper function for making an http GET request.
func (c *Client) Get(ctx context.Context, rawurl string, out any, options ...RequestOption) error {
	return c.do(ctx, rawurl, http.MethodGet, nil, out, options...)
}

// helper function for making an http POST request.
func (c *Client) Post(ctx context.Context, rawurl string, in, out any, options ...RequestOption) error {
	return c.do(ctx, rawurl, http.MethodPost, in, out, options...)
}

// helper function for making an http PATCH request.
func (c *Client) Patch(ctx context.Context, rawurl string, in, out any, options ...RequestOption) error {
	return c.do(ctx, rawurl, http.MethodPatch, in, out, options...)
}

// helper function for making an http DELETE request.
func (c *Client) Delete(ctx context.Context, rawurl string, options ...RequestOption) error {
	return c.do(ctx, rawurl, http.MethodDelete, nil, nil, options...)
}

// helper function to make an http request.
func (c *Client) do(ctx context.Context, rawurl, method string, in, out any, options ...RequestOption) error {
	// executes the http request and returns the body as
	// and io.ReadCloser
	body, err := c.stream(ctx, rawurl, method, in, options...)
	if body != nil {
		defer func(body io.ReadCloser) {
			_ = body.Close()
		}(body)
	}
	if err != nil {
		return err
	}

	// if a json response is expected, parse and return
	// the json response.
	if out != nil {
		return json.NewDecoder(body).Decode(out)
	}
	return nil
}

// helper function to stream a http request.
func (c *Client) stream(ctx context.Context, rawurl, method string, in any, options ...RequestOption) (io.ReadCloser, error) {
	uri, err := url.JoinPath(c.base, rawurl)
	if err != nil {
		return nil, err
	}

	// if we are posting or putting data, we need to
	// write it to the body of the request.
	var buf io.ReadWriter
	if in != nil {
		buf = &bytes.Buffer{}
		// if posting form data, encode the form values.
		if form, ok := in.(*url.Values); ok {
			if _, err = io.WriteString(buf, form.Encode()); err != nil {
				log.Printf("in stream method err: %v", err)
			}
		} else if err = json.NewEncoder(buf).Encode(in); err != nil {
			return nil, err
		}
	}

	// creates a new http request.
	req, err := http.NewRequestWithContext(ctx, method, uri, buf)
	if err != nil {
		return nil, err
	}

	for _, opt := range options {
		opt.Apply(req)
	}

	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if _, ok := in.(*url.Values); ok {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	// send the http request.
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(resp.Body)

		errorResponse := ErrorResponse{}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&errorResponse); decodeErr != nil {
			return nil, decodeErr
		}

		message := strings.TrimSpace(errorResponse.Payload)
		if message == "" {
			message = "Empty response from server."
		}

		return nil, errorResponse
	}
	return resp.Body, nil
}

type ErrorResponse struct {
	Status  int
	Payload string
}

func (r ErrorResponse) Error() string {
	return fmt.Sprintf("error occurred with status code %d and payload %s", r.Status, r.Payload)
}
