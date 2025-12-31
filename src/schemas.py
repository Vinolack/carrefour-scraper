from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Dict, Any, Literal

class TaskSubmitRequest(BaseModel):
    type: Literal["product", "store"] = Field(
        ..., 
        description="任务类型：'product' (直接抓取商品链接) 或 'store' (抓取店铺/分类页)"
    )
    urls: List[str] = Field(
        ..., 
        description="链接列表。如果是 store 类型，放入店铺或分类 URL；如果是 product 类型，放入商品 URL"
    )
    pages: Optional[int] = Field(
        1, 
        ge=1, 
        description="[仅 store 类型有效] 需要爬取的页码数量，默认为 1"
    )
class TaskSubmitResponse(BaseModel):
    task_id: str
    message: str
    status_url: str

class ProductData(BaseModel):
    Product_URL: str
    Title: Optional[str] = None
    Price: Optional[str] = None
    Seller: Optional[str] = None
    # 允许包含其他任意字段
    model_config = {"extra": "allow"}

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str  # pending, scanning_pages, scraping_products, completed, failed
    progress: str # 描述性进度文本
    created_at: str
    completed_at: Optional[str] = None
    results_count: int = 0
    results: List[Dict[str, Any]] = []