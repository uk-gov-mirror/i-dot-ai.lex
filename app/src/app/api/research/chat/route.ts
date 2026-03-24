import { createAzure } from '@ai-sdk/azure';
import { createMCPClient } from '@ai-sdk/mcp';
import { streamText, convertToModelMessages, stepCountIs } from 'ai';
import { after } from 'next/server';
import { langfuseSpanProcessor } from '../../../../../instrumentation';
import { buildResearchSystemPrompt } from '@/lib/research-prompt';

// Initialise Azure OpenAI provider
const azure = createAzure({
  resourceName: process.env.AZURE_OPENAI_ENDPOINT!.match(/https:\/\/(.+?)\.openai\.azure\.com/)?.[1] || '',
  apiKey: process.env.AZURE_OPENAI_API_KEY!,
  apiVersion: process.env.AZURE_OPENAI_API_VERSION,
});

// System prompt composed from shared SKILL.md + web-specific wrapper
const SYSTEM_PROMPT = buildResearchSystemPrompt();

// Server-side API URL (no CORS issues)
const API_URL = process.env.API_URL || 'http://localhost:8000';

// MCP tool names exposed by the Lex backend for research
const RESEARCH_MCP_TOOLS = [
  'search_for_legislation_sections',
  'search_for_legislation_acts',
  'search_amendments',
  'search_amendment_sections',
  'search_explanatory_note',
  'get_explanatory_note_by_legislation',
  'get_explanatory_note_by_section',
  'lookup_legislation',
];

export async function POST(req: Request) {
  let mcpClient: Awaited<ReturnType<typeof createMCPClient>> | null = null;

  try {
    const { messages, maxSteps = 10 } = await req.json();

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const tools: Record<string, any> = {};

    // Research tools via MCP — auto-synced with backend schemas
    mcpClient = await createMCPClient({
      transport: { type: 'http', url: `${API_URL}/mcp` },
    });

    const allMcpTools = await mcpClient.tools();
    for (const name of RESEARCH_MCP_TOOLS) {
      if (allMcpTools[name]) {
        tools[name] = allMcpTools[name];
      }
    }

    // Stream response with GPT-5-mini reasoning
    const result = streamText({
      model: azure.responses(process.env.AZURE_OPENAI_CHAT_DEPLOYMENT || 'gpt-5-mini'),
      system: SYSTEM_PROMPT,
      messages: await convertToModelMessages(messages),
      tools,
      stopWhen: stepCountIs(maxSteps),
      experimental_telemetry: {
        isEnabled: true,
        functionId: 'research-chat',
      },
      providerOptions: {
        openai: {
          reasoning_effort: 'low',
          reasoningSummary: 'detailed',
        },
      },
      async onFinish() {
        await mcpClient?.close();
      },
      onError({ error }) {
        console.error('streamText error:', error);
        mcpClient?.close();
      },
    });

    // Flush Langfuse traces after response completes
    after(async () => {
      await langfuseSpanProcessor.forceFlush();
    });

    return result.toUIMessageStreamResponse({
      sendReasoning: true,
    });
  } catch (error) {
    await mcpClient?.close();
    console.error('Deep research error:', error);
    return new Response(
      JSON.stringify({
        error: 'Failed to process research query',
        details: error instanceof Error ? error.message : 'Unknown error',
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      },
    );
  }
}
