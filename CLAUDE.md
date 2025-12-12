# cds-softdelete-plugin-java Quick Reference

## Project Structure

**Plugin**: `/cds-feature-softdelete` - CAP Java soft delete plugin (git repository here)
**Test Project**: 
`/cds-softdelete-test` - Test project with 17 tests
`/spec-test` - Test project based on `docs/project/spec.md`

## Quick Commands

### Build Plugin and Run Tests
```bash
cd /home/miyasuta/projects/cds-softdelete-plugin-java/cds-feature-softdelete && \
JAVA_HOME=/usr/lib/jvm/sapmachine-jdk-21.0.4 mvn clean install -DskipTests && \
cd ../cds-softdelete-test && \
JAVA_HOME=/usr/lib/jvm/sapmachine-jdk-21.0.4 mvn test
```

### Run Specific Test
```bash
cd /home/miyasuta/projects/cds-softdelete-plugin-java/cds-softdelete-test
JAVA_HOME=/usr/lib/jvm/sapmachine-jdk-21.0.4 mvn test -Dtest=OrderDraftServiceTest#navigationToItemsShouldFilterDeletedRecords
```

### Capture Test Logs
```bash
# BEST PRACTICE: Redirect to file first, then grep
# This avoids issues with tee and allows multiple grep operations
mvn test > /tmp/test-output.log 2>&1
grep "READ request" /tmp/test-output.log

# ALTERNATIVE: Use tee for real-time output (but redirect stderr first)
mvn test 2>&1 | tee /tmp/test-output.log

# ❌ WRONG: Direct pipe misses stderr (Spring Boot logs)
mvn test | grep "pattern"

# ✅ CORRECT: Redirect stderr to stdout before piping
mvn test 2>&1 | grep "pattern"
```

## Essential Rules

### Git Workflow
- Always create new branch for features
- Commit messages: single line, descriptive
- Delete merged branches
- Git repository is in `/cds-feature-softdelete`

### CAP Java Type Safety
- Use typed entity classes (Books, Orders) — never Map<String,Object>
- Handle results via .listOf(Foo.class) or .single(Foo.class)
- Use generated service interfaces (CatalogService) — not generic CdsService
- Build queries with metamodel constants (Books_.CDS_NAME)

### MCP Server Usage
- **MUST** search CDS definitions with cds-mcp before reading .cds files
- **MUST** search CAP docs with cds-mcp before modifying CDS models or using CAP APIs
- Call UI5 get_guidelines before any UI5 work

## Key Implementation Notes

### Handler Registration
- Use `@On` with `@HandlerOrder(HandlerOrder.EARLY)` for DELETE operations
- Equivalent to Node.js `srv.prepend()`

### Database Operations
- Use `PersistenceService` for direct DB operations, not `ApplicationService`
- Extract DB entity name: `entity.query().get().ref().firstSegment()`

### DELETE Handler
- Must return row count: `ResultBuilder.deletedRows(count).result()`
- Returning empty list causes 404 "Entity not found"

### Draft Entity Filtering
- Skip filtering for draft tables (`_drafts`) and draft records (`IsActiveEntity=false`)
- Apply filtering to active entity navigation (e.g., `Orders(...,IsActiveEntity=true)/items`)

### By-Key Access Detection
- Single segment + filter = by-key access (e.g., `Orders(ID=1)`)
- Multiple segments = navigation path (e.g., `Orders(ID=1)/items`)

### $expand Filtering
- For by-key access: Query parent's `isDeleted` value, propagate to children
- For `$filter=isDeleted eq true`: Propagate to expand filters
- Default: Apply `isDeleted=false` to expand filters

## Critical Debugging Tip

**Always redirect stderr when capturing logs**:

```bash
# ❌ WRONG - misses Spring Boot logs (they go to stderr)
mvn test | grep "pattern"

# ✅ CORRECT - captures both stdout and stderr
mvn test 2>&1 | grep "pattern"

# ✅ BEST - save to file first (most flexible)
mvn test > /tmp/test-output.log 2>&1
grep "pattern1" /tmp/test-output.log
grep "pattern2" /tmp/test-output.log  # Can grep multiple times
```

**Why**: Spring Boot logs to stderr, Maven output to stdout. Without `2>&1`, pipes only see stdout.

**Real Example**:
```bash
# Run specific test and save output
cd /home/miyasuta/projects/cds-softdelete-plugin-java/spec-test
JAVA_HOME=/usr/lib/jvm/sapmachine-jdk-21.0.4 mvn test -Dtest=ActivationTest#act01* > /tmp/act01.log 2>&1

# Analyze the results
grep -E "Tests run:|FAILURE|SUCCESS" /tmp/act01.log
grep "Active entity check" /tmp/act01.log
grep "Draft child delete" /tmp/act01.log
```

## Reference Implementation

Node.js version: https://github.com/miyasuta/cds-softdelete-plugin

## Detailed Documentation

For comprehensive information, see:

### Reusable Guides (applicable to any CAP Java project)
- [Development Guide](docs/guides/development-guide.md) - Build, test, Git workflow, event handlers
- [Debugging Guide](docs/guides/debugging-guide.md) - Log analysis, troubleshooting, performance debugging
- [Coding Standards](docs/guides/coding-standards.md) - Type safety rules, naming conventions, best practices

### Project-Specific Documentation
- [Architecture](docs/project/architecture.md) - Component structure, core functionality, limitations
- [Implementation Notes](docs/project/implementation-notes.md) - Design decisions, lessons learned, known issues
- [Test Strategy](docs/project/test-strategy.md) - Test plan, entity model, assertions, debugging tests

## SAP Development Rules (for AI Assistants)

### CAP MCP Server (@cap-js/mcp-server)
- Search for CDS definitions (entities, fields, services) with cds-mcp
- Search for CAP docs with cds-mcp EVERY TIME modifying CDS or using CAP APIs
- Only read .cds files if MCP fails
- Refer to @cap docs for SAP CAP applications

### Fiori MCP Server (@sap-ux/fiori-mcp-server)
- Use when creating/modifying SAP Fiori elements apps
- Follow 3-step workflow: list-functionality → get-functionality-details → execute-functionality
- Modify code, not screen personalization

### UI5 MCP Server (@ui5/mcp-server)
- **MUST** call once to retrieve UI5 guidelines before any UI5 work
- Run linter after changes and verify no new problems

## CAP Development Rules

### Project Initialization
- Use `cds init` (not `cds init projectname`)
- Create nodejs CAP applications (don't add --add)
- Add `cds lint` after generating
- Enable draft but AVOID on composed entities
- Don't add random samples with `cds add sample`
- Set up npm workspaces for UI5:
  ```json
  {
    "workspaces": ["app/*"]
  }
  ```
