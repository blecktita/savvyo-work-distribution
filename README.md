# Distributed Work Management System

A distributed work management platform for coordinating automated data collection and processing tasks across multiple worker nodes.

## Architecture Overview

This system enables efficient distribution and management of automated data collection tasks across a distributed workforce. The platform maintains work order queues and provides real-time coordination between host controllers and worker processors.

## Directory Structure

- `available_work/` - Work orders awaiting assignment to available workers
- `active_work/` - Work orders currently in progress by assigned workers  
- `completed_work/` - Successfully completed work orders with deliverables
- `retry_queue/` - Failed work orders queued for reprocessing

## System Components

### Control Node (Orchestration & Management)
The control node manages work distribution, monitors progress, and maintains the central work queue.

```bash
uv run --active control_manager.py --repository-url <github-repo-url>
```

### Processing Nodes (Task Execution)
Worker nodes claim available work orders and execute automated data collection tasks.

```bash
uv run --active worker_processor.py --repository-url <github-repo-url>
```

## Work Order Specification

Each work order follows a standardized JSON format for consistent processing:

```json
{
  "work_id": "task_GB1_uuid",
  "target_identifier": "GB1", 
  "target_endpoint": "https://...",
  "processed_datasets": ["2020", "2021"],
  "created_timestamp": "2025-07-16T10:30:00Z",
  "priority": "standard",
  "estimated_duration": "2h"
}
```

## Key Features

- **Distributed Processing**: Automatically distributes workload across available worker nodes
- **Fault Tolerance**: Failed tasks are automatically queued for retry with exponential backoff
- **Progress Monitoring**: Real-time visibility into work order status and completion rates
- **Scalable Architecture**: Supports dynamic scaling of worker nodes based on demand
- **Quality Assurance**: Built-in validation and verification of completed work orders

## Getting Started

1. Clone the repository to your control node and worker machines
2. Configure authentication credentials for repository access
3. Start the control manager on your primary coordination node
4. Deploy worker processors across your available compute resources
5. Monitor progress through the management dashboard

## Configuration

Customize system behavior through environment variables or configuration files:

- `MAX_CONCURRENT_WORKERS`: Maximum simultaneous work orders per node
- `RETRY_ATTEMPTS`: Number of retry attempts for failed tasks
- `POLLING_INTERVAL`: Frequency of work queue checks
- `RESULT_VALIDATION`: Enable/disable output validation

## Support

For technical support and implementation guidance, please contact the platform engineering team.