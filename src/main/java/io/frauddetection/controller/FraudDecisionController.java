package io.frauddetection.controller;

import io.frauddetection.model.dto.AcknowledgeRequest;
import io.frauddetection.model.dto.FraudAlertResponse;
import io.frauddetection.model.dto.FraudDecisionResponse;
import io.frauddetection.service.FraudDecisionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/fraud")
@RequiredArgsConstructor
@Validated
@Tag(
    name        = "Fraud Decisions",
    description = "Retrieve fraud evaluation decisions and manage fraud alert acknowledgements"
)
public class FraudDecisionController {

    private final FraudDecisionService fraudDecisionService;

    // ── GET /decisions/{transactionId} ────────────────────────────────────────

    @GetMapping("/decisions/{transactionId}")
    @Operation(
        summary     = "Get fraud decision by transaction ID",
        description = "Returns the complete fraud evaluation result — status, score, triggered rules "
                    + "— for the specified transaction."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Decision found",
            content = @Content(schema = @Schema(implementation = FraudDecisionResponse.class))),
        @ApiResponse(responseCode = "404", description = "No transaction with that ID exists",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    })
    public ResponseEntity<FraudDecisionResponse> getByTransactionId(
            @Parameter(description = "Unique transaction identifier", required = true,
                       example = "TXN-20240101-001")
            @PathVariable String transactionId) {

        return ResponseEntity.ok(fraudDecisionService.getByTransactionId(transactionId));
    }

    // ── GET /decisions?accountId=&page=&size=&sort= ───────────────────────────

    @GetMapping("/decisions")
    @Operation(
        summary     = "List fraud decisions for an account",
        description = "Returns a paginated list of fraud decisions for the given account, "
                    + "defaulting to 20 results per page sorted by creation date descending. "
                    + "Supports standard Spring pagination parameters: page, size, sort."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Paginated decisions returned"),
        @ApiResponse(responseCode = "400", description = "accountId is missing or blank",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    })
    public ResponseEntity<Page<FraudDecisionResponse>> getByAccountId(
            @Parameter(description = "Account ID to filter decisions", required = true,
                       example = "ACC-001234")
            @RequestParam @NotBlank String accountId,
            @ParameterObject
            @PageableDefault(size = 20, sort = "createdAt", direction = Sort.Direction.DESC)
            Pageable pageable) {

        return ResponseEntity.ok(fraudDecisionService.getByAccountId(accountId, pageable));
    }

    // ── POST /decisions/acknowledge/{alertId} ─────────────────────────────────

    @PostMapping("/decisions/acknowledge/{alertId}")
    @Operation(
        summary     = "Acknowledge a fraud alert",
        description = "Marks a fraud alert as reviewed and acknowledged by an operator. "
                    + "Once acknowledged the operation is idempotent-safe — a second call "
                    + "returns 409 Conflict."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Alert acknowledged successfully",
            content = @Content(schema = @Schema(implementation = FraudAlertResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request body",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class))),
        @ApiResponse(responseCode = "404", description = "Alert not found",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class))),
        @ApiResponse(responseCode = "409", description = "Alert has already been acknowledged",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    })
    public ResponseEntity<FraudAlertResponse> acknowledgeAlert(
            @Parameter(description = "UUID of the fraud alert to acknowledge", required = true)
            @PathVariable UUID alertId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Operator identifier performing the acknowledgement",
                required    = true,
                content     = @Content(schema = @Schema(implementation = AcknowledgeRequest.class)))
            @RequestBody @Valid AcknowledgeRequest request) {

        return ResponseEntity.ok(fraudDecisionService.acknowledgeAlert(alertId, request));
    }
}
