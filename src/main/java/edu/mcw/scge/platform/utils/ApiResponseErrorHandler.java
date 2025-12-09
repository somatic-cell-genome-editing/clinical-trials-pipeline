package edu.mcw.scge.platform.utils;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResponseErrorHandler;

import java.io.IOException;
import java.net.URI;

public class ApiResponseErrorHandler implements ResponseErrorHandler {
    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return (response.getStatusCode().is4xxClientError() || response.getStatusCode().is5xxServerError());
    }

    @Override
    public void handleError(URI url, HttpMethod method, ClientHttpResponse response) throws IOException {
        if (response.getStatusCode().is4xxClientError()) {
            // Handle 4xx errors
            if (response.getStatusCode() == HttpStatus.NOT_FOUND) {
                // Handle 404 Not Found specifically
                try {
                    throw new Exception("Resource not found");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            // Handle other 4xx errors
            throw new HttpClientErrorException(response.getStatusCode(), response.getStatusText());

        } else if (response.getStatusCode().is5xxServerError()) {
            // Handle 5xx errors
            throw new HttpServerErrorException(response.getStatusCode(), response.getStatusText());
        }
    }

}
