# Nozzle Studio Playground - Technical Documentation for Claude

## Application Overview

Nozzle Studio Playground is a React-based web application that provides a SQL query interface for exploring and analyzing blockchain data extracted by Project Nozzle. It serves as the frontend companion to the Nozzle ETL architecture, enabling users to:

- Write and execute SQL queries against blockchain datasets
- Browse available event schemas from smart contracts
- Explore dataset metadata and column information
- Utilize custom User-Defined Functions (UDFs) for blockchain-specific operations
- Visualize query results in tabular format

The application connects to the Nozzle backend services via HTTP and gRPC protocols (through Connect-Web), providing a seamless interface for data exploration.

## Architecture Analysis

### React Patterns & State Management

The application follows modern React patterns (React 19) with a focus on:

1. **Functional Components**: All components are functional with hooks
2. **Suspense & Concurrent Features**: Leverages React Suspense for data loading
3. **Custom Hooks**: Encapsulates business logic in reusable hooks
4. **Form Management**: Uses TanStack Form for complex form state
5. **Query State**: TanStack Query for server state management

### Component Structure

```
Components/
├── QueryPlayground/        # Main query interface components
│   ├── QueryPlaygroundWrapper.tsx  # Main container with tabs
│   ├── Editor.tsx          # Monaco SQL editor wrapper
│   ├── DatasetQueryResultTable.tsx # Results table with TanStack Table
│   ├── SchemaBrowser.tsx   # Event schema explorer
│   ├── MetadataBrowser.tsx # Dataset metadata viewer
│   └── UDFBrowser.tsx      # UDF documentation browser
└── Form/                   # Form utilities
    ├── form.ts            # Form context setup
    └── ErrorMessages.tsx   # Error display component
```

### Data Flow

1. **Router Context**: Provides QueryClient and Transport to all routes
2. **Prefetching**: Route loaders prefetch data before rendering
3. **Streaming**: Uses Server-Sent Events (SSE) for real-time schema updates
4. **Query Execution**: Effect-based Arrow Flight client for SQL execution

## Code Organization

### Directory Structure

```
typescript/studio/
├── src/
│   ├── clients/           # API clients and transport setup
│   │   ├── Nozzle.ts     # Arrow Flight query runner
│   │   └── Router.tsx    # App router with QueryClient
│   ├── Components/       # React components
│   ├── hooks/           # Custom React hooks
│   ├── routes/          # File-based routing
│   ├── utils/           # Utility functions
│   ├── constants.ts     # App constants (API endpoints)
│   ├── env.ts          # Environment configuration
│   ├── main.tsx        # App entry point
│   └── styles.css      # Global styles with Tailwind
├── tests/              # Test files
├── public/             # Static assets
└── Configuration files
```

### Component Organization Principles

- **Colocation**: Components with their specific logic are grouped together
- **Separation of Concerns**: Business logic in hooks, UI in components
- **Type Safety**: Full TypeScript coverage with strict mode
- **Modularity**: Each component is self-contained with clear interfaces

## Key Components

### QueryPlaygroundWrapper

- **Purpose**: Main container for the query interface
- **Features**:
  - Tab management for multiple queries
  - Form state management with TanStack Form
  - Command key detection (CMD/CTRL + Enter to run)
- **State**: Local state for tabs, form state for queries

### Editor (Monaco Editor Wrapper)

- **Purpose**: SQL editor with syntax highlighting
- **Integration**: Wrapped as a TanStack Form field component
- **Features**: Error display, touch state tracking

### DatasetQueryResultTable

- **Purpose**: Display query results in a sortable, filterable table
- **Technology**: TanStack Table v8 with pagination
- **Current State**: Uses demo data (needs backend integration)

### SchemaBrowser

- **Purpose**: Display queryable events from smart contracts
- **Data Source**: SSE stream from `/api/v1/events/stream`
- **UI Pattern**: Accordion with expandable event parameters

### MetadataBrowser

- **Purpose**: Show dataset metadata columns available for queries
- **Features**: Collapsible panel showing column names and data types

### UDFBrowser

- **Purpose**: Documentation for User-Defined Functions
- **Display**: Accordion with function signatures and descriptions

## Data Layer

### Custom Hooks

#### useQueryableEventsQuery

- **Type**: Custom SSE hook (not using TanStack Query)
- **Purpose**: Real-time event schema updates
- **Features**: Auto-reconnect, error handling, cleanup
- **Pattern**: Reducer-based state management

#### useMetadataQuery

- **Type**: TanStack Query hook
- **Purpose**: Fetch dataset metadata
- **Pattern**: Query options for prefetching

#### useUDFQuery

- **Type**: TanStack Query hook
- **Purpose**: Provide UDF documentation
- **Note**: Currently uses static data

#### useOSQuery

- **Type**: TanStack Query hook
- **Purpose**: Detect operating system for keyboard shortcuts

### API Integration

#### Nozzle Client (`clients/Nozzle.ts`)

```typescript
// Arrow Flight integration using Effect
const program = Effect.gen(function* () {
  const flight = yield* ArrowFlight.ArrowFlight
  const table = yield* flight.table(query)
  // Convert Arrow table to JSON
})
```

#### Transport Setup

- Connect-Web transport for gRPC communication
- Base URL: `/nozzle` (proxied to backend)

## Routing

### TanStack Router v1

- **Type**: File-based routing with type safety
- **Configuration**: Auto code-splitting enabled
- **Route Structure**:
  ```
  __root.tsx  - Root layout with QueryClient provider
  index.tsx   - Main playground page
  ```

### Route Context

Provides shared resources to all routes:

- `queryClient`: TanStack Query client
- `transport`: Connect transport for gRPC

### Data Prefetching

Routes use loaders to prefetch data:

```typescript
async loader({ context }) {
  await context.queryClient.ensureQueryData(osQueryOptions)
  await context.queryClient.ensureQueryData(udfQueryOptions)
  await context.queryClient.ensureQueryData(metadataQueryOptions)
}
```

## UI/UX Patterns

### Component Patterns

1. **Base UI Components**: Unstyled, accessible components
   - Accordion, Tabs, Collapsible
   - Follows WAI-ARIA patterns

2. **Tailwind CSS v4**: Utility-first styling
   - Dark theme by default
   - Custom theme with Space Mono font

3. **Phosphor Icons**: Consistent icon system

### User Interaction Flows

1. **Query Writing**:
   - User types SQL in Monaco editor
   - Syntax highlighting provides feedback
   - CMD/CTRL + Enter executes query

2. **Schema Exploration**:
   - Browse events in accordion
   - Click to expand parameters
   - Copy column names to query

3. **Tab Management**:
   - Multiple query tabs
   - Click "+" to add new tab
   - Switch between queries

## Development Standards

### TypeScript Conventions

1. **Strict Mode**: Full type safety enabled
2. **Type Imports**: Use `type` imports for types
3. **Const Assertions**: Use `as const` for literals
4. **Generic Components**: Properly typed with generics

### React Best Practices

1. **Hooks Rules**: Follow Rules of Hooks
2. **Memoization**: Use `useMemo` for expensive computations
3. **Effect Cleanup**: Always clean up subscriptions
4. **Error Boundaries**: Handle errors gracefully

### Code Style

1. **ESLint**: TanStack config with strict rules
2. **Prettier**: Consistent formatting
3. **Imports**: Organized with aliases (`@/`)

## Query Playground Features

### Current Capabilities

1. **SQL Editor**: Full SQL syntax support with Monaco
2. **Schema Browser**: View available events and parameters
3. **Metadata Explorer**: Browse dataset columns
4. **UDF Documentation**: Reference for custom functions
5. **Multi-tab Support**: Work with multiple queries

### Planned Features (TODOs in code)

1. **Query Execution**: Connect to backend for real queries
2. **Result Visualization**: Display actual query results
3. **Column Insertion**: Click to add columns to query
4. **UDF Insertion**: Add UDFs to query editor
5. **Query History**: Track and reuse previous queries

## Testing Approach

### Testing Stack

- **Vitest**: Test runner with Jest compatibility
- **Testing Library**: React testing utilities
- **JSDOM**: Browser environment simulation

### Test Organization

```
tests/
└── hooks/
    └── useQueryableEventsQuery.spec.tsx
```

### Testing Patterns

1. **Hook Testing**: Using `renderHook` from Testing Library
2. **SSE Mocking**: Custom EventSource mock implementation
3. **Async Testing**: `waitFor` for async state changes
4. **Coverage**: Tests for success, error, and edge cases

### Example Test Pattern

```typescript
const { result } = renderHook(() => useQueryableEventsQuery())
await waitFor(() => {
  expect(result.current.isSuccess).toBe(true)
})
```

## Build System

### Vite Configuration

```typescript
{
  plugins: [
    tanstackRouter(),  // File-based routing
    viteReact(),       // React Fast Refresh
    tailwindcss(),     // Tailwind v4
  ],
  resolve: {
    alias: { "@": "./src" }  // Path aliases
  }
}
```

### Build Process

1. **Development**: `pnpm dev` - Vite dev server on port 5173
2. **Production**: `pnpm build` - Optimized bundle
3. **Type Check**: TypeScript compilation post-build

## Environment Configuration

### T3 Env Setup

- Type-safe environment variables
- Zod validation schemas
- Client/Server variable separation

### Environment Variables

```typescript
// Client variables (VITE_ prefix)
VITE_APP_TITLE: Optional app title

// Server variables
SERVER_URL: Optional server URL
```

### API Configuration

- Default API: `http://localhost:3000/api/v1`
- Configurable via constants.ts

## Integration Points

### Backend Services

1. **Arrow Flight Server** (port 1602)
   - SQL query execution
   - Binary data transfer
   - Accessed via `/nozzle` proxy

2. **HTTP API** (port 3000)
   - `/api/v1/metadata` - Dataset metadata
   - `/api/v1/events/stream` - SSE for schemas
   - `/api/v1/os` - OS detection

3. **Data Flow**:
   ```
   UI → TanStack Query → HTTP/SSE → Backend
   UI → Effect/Arrow → Connect-Web → gRPC → Backend
   ```

### NozzleStudioApi API Endpoints

**Base URL:** `http://localhost:3000/api/v1`

#### Core Endpoints:

1. **GET `/metadata`**
   - **Purpose**: Fetch dataset metadata and schema information
   - **Response**: JSON containing `DatasetMetadata` with schema definitions
   - **Usage**: Used by the Schema Browser to display available datasets and their structures
   - **Implementation**: `useMetadataQuery.tsx:13`

2. **GET `/events/stream`** (Server-Sent Events)
   - **Purpose**: Real-time streaming of queryable blockchain events
   - **Protocol**: SSE (Server-Sent Events) for live data streaming
   - **Response**: Stream of `QueryableEventStream` data containing blockchain events
   - **Features**: Auto-reconnect with configurable retry logic
   - **Implementation**: `useQueryableEventsQuery.tsx:128`

#### Data Flow Architecture:

- **HTTP REST**: Traditional request/response for metadata
- **SSE Streaming**: Real-time event data with automatic reconnection
- **Type Safety**: All responses validated using Effect Schema decoders
- **Error Handling**: Comprehensive error states and retry mechanisms

#### Integration Points:

- **Port Configuration**: Default port 3000 (configurable via studio command)
- **CORS Support**: Required for frontend integration
- **Schema Validation**: Uses Effect Schema for runtime type checking
- **State Management**: TanStack Query for HTTP, custom reducer for SSE

### External Dependencies

1. **nozzl Package**: Shared types and Arrow Flight client
2. **Effect**: Functional programming for data operations
3. **Connect-Web**: gRPC-Web protocol support

## Performance Considerations

### Optimization Strategies

1. **Code Splitting**: Auto-splitting per route
2. **Lazy Loading**: Components loaded on demand
3. **Query Caching**: TanStack Query with stale-while-revalidate
4. **Virtual Scrolling**: Planned for large result sets

### Memory Management

1. **SSE Cleanup**: Proper EventSource cleanup
2. **Query Garbage Collection**: Automatic via TanStack Query
3. **Component Unmounting**: Effect cleanup in hooks

### Bundle Optimization

1. **Tree Shaking**: Vite production builds
2. **Minification**: Automatic in production
3. **Dependency Optimization**: Vite pre-bundling

## Common Development Patterns

### Adding a New Query Hook

```typescript
export const myQueryOptions = queryOptions({
  queryKey: ["Query", "MyData"] as const,
  async queryFn() {
    const response = await fetch(`${API_ORIGIN}/my-endpoint`)
    return Schema.decodeUnknownSync(MySchema)(await response.json())
  },
})

export function useMyQuery() {
  return useQuery(myQueryOptions)
}
```

### Adding a Browser Component

```typescript
export function MyBrowser() {
  const { data } = useMyDataQuery()

  return (
    <Accordion.Root>
      {data.map(item => (
        <Accordion.Item key={item.id}>
          {/* Component content */}
        </Accordion.Item>
      ))}
    </Accordion.Root>
  )
}
```

### Integrating with Form

```typescript
const form = useAppForm({
  defaultValues: { field: "" },
  validators: {
    onChange: Schema.standardSchemaV1(FormSchema),
  },
  async onSubmit({ value }) {
    // Handle submission
  },
})
```

## Debugging Tips

1. **React DevTools**: Component tree inspection
2. **TanStack DevTools**: Query and router debugging
3. **Network Tab**: Monitor SSE connections
4. **Console Logging**: Effect traces for data flow

## Future Enhancements

Based on code TODOs and architecture:

1. **Query Execution**: Complete backend integration
2. **Visual Query Builder**: Drag-drop interface
3. **Result Export**: CSV/JSON download
4. **Query Templates**: Saved query library
5. **Collaborative Features**: Share queries
6. **Performance Monitoring**: Query execution metrics
7. **Advanced Visualizations**: Charts and graphs
8. **Keyboard Shortcuts**: Power user features
9. **Query Validation**: Pre-execution checks
10. **Auto-completion**: Schema-aware suggestions

## Getting Started for Developers

### Prerequisites

```bash
# Install dependencies
pnpm install

# Start Nozzle backend (required)
cargo run --release -p nozzle -- server

# Start development server
pnpm dev
```

### Common Tasks

1. **Add a new route**: Create file in `src/routes/`
2. **Add a component**: Create in `src/Components/`
3. **Add a hook**: Create in `src/hooks/`
4. **Add API integration**: Update `clients/Nozzle.ts`
5. **Add types**: Import from `nozzl` package

### Code Quality

```bash
pnpm lint      # Run ESLint
pnpm format    # Run Prettier
pnpm test      # Run tests
pnpm check     # Lint + Format
pnpm build     # Type check + Build
```

## Important Notes

1. **Backend Dependency**: Requires Nozzle server running
2. **Demo Data**: Currently shows hardcoded results
3. **SSE Connection**: Auto-reconnects on failure
4. **Type Safety**: Full TypeScript coverage required
5. **Accessibility**: Follow WAI-ARIA patterns
