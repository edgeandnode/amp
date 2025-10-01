/**
 * Global type definitions for Monaco Editor environment
 */

/**
 * Monaco Editor Environment Configuration
 *
 * This interface defines the environment configuration for Monaco Editor's web workers.
 * Monaco Editor uses web workers for various language services to keep the main thread responsive.
 */
interface MonacoEnvironment {
  /**
   * Gets a web worker for Monaco Editor services
   *
   * @param workerId - Optional identifier for the type of worker needed
   * @param label - Optional label for the worker
   * @returns A web worker instance for Monaco Editor to use
   */
  getWorker(workerId?: string, label?: string): Worker

  /**
   * Optional: Gets the worker URL for a specific module
   * This can be used to specify custom worker URLs for different services
   */
  getWorkerUrl?(moduleId: string, label: string): string

  /**
   * Optional: Base URL for loading Monaco Editor resources
   */
  baseUrl?: string
}

/**
 * Extend the global scope to include MonacoEnvironment
 */
declare global {
  interface Window {
    MonacoEnvironment?: MonacoEnvironment
  }

  interface WorkerGlobalScope {
    MonacoEnvironment?: MonacoEnvironment
  }

  interface GlobalThis {
    MonacoEnvironment?: MonacoEnvironment
  }

  // Allow direct assignment to self (for web worker context)
  var MonacoEnvironment: MonacoEnvironment | undefined
}

export {}
