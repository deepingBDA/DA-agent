import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from mcp_use import MCPAgent, MCPClient
import pytz

async def main():
    # Load environment variables
    load_dotenv()

    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
    CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

    # Create configuration dictionary
    config = {
        "mcpServers": {
            "insight": {
                "command": "python3",
                "args": [
                    "mcp_insight.py"
                ]
            },
            "clickhouse": {
                "command": "python3",
                "args": [
                    "mcp_clickhouse.py"
                ]
            },
            "sequential-thinking": {
                "command": "npx",
                "args": [
                    "-y",
                    "@modelcontextprotocol/server-sequential-thinking"
                ]
            },
            "memory": {
                "command": "npx",
                "args": [
                    "-y",
                    "@modelcontextprotocol/server-memory"
                ]
            }
        }
    }

    # Create MCPClient from configuration dictionary
    client = MCPClient.from_dict(config)

    # Create LLM
    model_name = "gpt-o4"
    llm = ChatOpenAI(model="gpt-o4")

    # Create agent with the client
    agent = MCPAgent(llm=llm, client=client, max_steps=50)

    # 한국 시간대(KST) 사용
    now = datetime.now(pytz.timezone('Asia/Seoul'))
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    prompt = "25년 3월 1일부터 25년 3월 31일까지, 역삼점 데이터베이스인 plusinsight_bgf_yeoksam에서 sales_funnel_pickup_rate tool을 이용해 분석을 하고, 이것을 pickup_transition 결과와 연관지어서 분석해줘. pickup transition은 한 zone에서 물건을 픽업했을 때 다른 zone의 물건도 픽업했는지에 대한 전환이야. 또한 representative_movement를 이용하면, 대표 이동 동선 정보를 알 수 있어. 이 정보도 픽업 분석과 연관지을 수 있으면 분석해줘. 구체적이고 창의적인 Actionable insight를 제시해야돼. 최종 답변은 한국어로 대답해줘. 구체적인 케이스 5건 이상과 함께, 보고서 형태로 작성해줘."

    # Run the query
    result = await agent.run(prompt)
    
    print(result)
    with open(f"report/insight_{now_str}.md", "a") as f:
        f.write(result)
        f.write(f"\n\n\n")

if __name__ == "__main__":
    asyncio.run(main())
