package com.gcp.examples.storage.s3api;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;

public class CustomGCPSigner extends AWS4Signer {
    @Override
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {
        request.addHeader("Authorization", "Bearer " + credentials.getAWSAccessKeyId());
    }
}