from fastapi import FastAPI, BackgroundTasks, HTTPException
from typing import Dict
import uuid
from datetime import datetime
import uvicorn
from src import schemas
from src import scraper

app = FastAPI(title="Carrefour Scraper Service API")

# 内存数据库
JOBS_DB: Dict[str, dict] = {}

@app.post("/tasks", response_model=schemas.TaskSubmitResponse, status_code=202)
async def submit_task(request: schemas.TaskSubmitRequest, background_tasks: BackgroundTasks):
    """
    提交抓取任务。
    - **type**: "product" (商品链接) 或 "store" (店铺链接)
    - **pages**: 仅当 type="store" 时有效，指定爬取多少页
    - **urls**: 链接列表
    """
    if not request.urls:
        raise HTTPException(status_code=400, detail="URL list cannot be empty")

    job_id = str(uuid.uuid4())
    
    # 初始化状态
    JOBS_DB[job_id] = {
        "task_id": job_id,
        "status": "pending",
        "progress": "Initializing...",
        "created_at": datetime.now().isoformat(),
        "results_count": 0,
        "results": []
    }

    # 提交到后台执行
    background_tasks.add_task(
        scraper.run_batch_job, 
        request.type, 
        request.urls, 
        request.pages, 
        JOBS_DB, 
        job_id
    )

    return {
        "task_id": job_id,
        "message": "Task submitted successfully",
        "status_url": f"/tasks/{job_id}"
    }

@app.get("/tasks/{task_id}", response_model=schemas.TaskStatusResponse)
async def get_task_status(task_id: str):
    if task_id not in JOBS_DB:
        raise HTTPException(status_code=404, detail="Task not found")
    return JOBS_DB[task_id]

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)