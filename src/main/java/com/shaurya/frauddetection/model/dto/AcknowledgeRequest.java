package com.shaurya.frauddetection.model.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AcknowledgeRequest {

    @NotBlank(message = "acknowledgedBy is required")
    private String acknowledgedBy;
}
