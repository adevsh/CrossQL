export type PipelineDefinition = {
  pipeline_id: string;
  pipeline_version: number;
  nodes: Array<{
    id: string;
    node_type: string;
    config: Record<string, unknown>;
  }>;
  edges: Array<{
    source: string;
    target: string;
  }>;
  sources: Record<
    string,
    {
      alias: string;
      source_type: 'PostgreSQL' | 'MySQL' | 'MongoDB' | 'Cassandra';
      partition_key: string | null;
      partition_hint: number | null;
    }
  >;
  execution: {
    chunk_size: number;
    max_concurrency: number;
    streaming: boolean;
  };
};

function slugify(input: string): string {
  return input
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'pipeline';
}

function sourceTypeForNode(nodeType: string): 'PostgreSQL' | 'MySQL' | 'MongoDB' | 'Cassandra' | null {
  if (nodeType === 'postgres') return 'PostgreSQL';
  if (nodeType === 'mysql') return 'MySQL';
  if (nodeType === 'mongodb') return 'MongoDB';
  if (nodeType === 'cassandra') return 'Cassandra';
  return null;
}

export function serializePipelineDefinition(
  nodes: any[],
  edges: any[],
  pipelineName: string
): PipelineDefinition {
  const sources: PipelineDefinition['sources'] = {};

  for (const node of nodes) {
    const sourceType = sourceTypeForNode(node.type);
    if (!sourceType) continue;
    const cfg = (node?.data?.config ?? {}) as Record<string, unknown>;
    sources[node.id] = {
      alias: node.id.toUpperCase(),
      source_type: sourceType,
      partition_key: typeof cfg.partition_key === 'string' ? cfg.partition_key : null,
      partition_hint: typeof cfg.partition_hint === 'number' ? cfg.partition_hint : null
    };
  }

  return {
    pipeline_id: slugify(pipelineName),
    pipeline_version: 1,
    nodes: nodes.map((node) => ({
      id: node.id,
      node_type: node.type,
      config: (node?.data?.config ?? {}) as Record<string, unknown>
    })),
    edges: edges.map((edge) => ({
      source: edge.source,
      target: edge.target
    })),
    sources,
    execution: {
      chunk_size: 50000,
      max_concurrency: 4,
      streaming: true
    }
  };
}
