# ADR-007: Security Strategy for Portfolio/Demo Environment

**Context:** The spec calls for reusing the "JWT pattern from URL Shortener" without specifying whether it relies on an external Identity Provider, or which endpoints require authentication. For a public portfolio project, requiring reviewers to configure an external IdP (Keycloak, Auth0, etc.) before they can run the project is a significant adoption barrier.

**Decision:**
- Use a locally-signed JWT (Spring Security OAuth2 Resource Server validating against a test signing key embedded/documented in the repo) instead of depending on an external IdP.
- **All endpoints require a valid JWT — GET included.** There is no unauthenticated read path.

**Rationale:** A reviewer must be able to `git clone`, `docker-compose up`, and exercise the full API without creating an account with a third-party service — that part of the original reasoning still holds and is why we stay away from an external IdP. But transaction records (amounts, account numbers, `businessId`) are financial data. No real transaction-processing system exposes read access to that data without authentication, even read-only — leaving `GET` open would contradict the project's own premise (financial-grade correctness and reliability) and would be a red flag to anyone reviewing this as a demonstration of production banking infrastructure. The "frictionless demo" goal is still met: generating a local test token is a single documented step, not an external dependency.

**Consequences:**
- The README must document how to generate a test token locally (e.g., a small script or `curl` against a local token-issuing endpoint, or a pre-generated example token with its expiry noted), since every request — including the curl examples in the Quickstart — needs one.
- This is explicitly a demo-grade security setup, not production-grade multi-tenant auth (no external IdP, no refresh token flow, no scopes/roles beyond a single "authenticated" claim). This tradeoff is intentional and should be called out in the README so it isn't mistaken for a production security posture.
- If this project were to become a real service, swapping the local JWT decoder for an external IdP's JWKS endpoint is a config-only change (Spring Security Resource Server already abstracts this).
