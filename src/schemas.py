from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal

class TaskSubmitRequest(BaseModel):

    type: Literal["product", "store", "repricing", "listing_price"] = Field(
        ..., 
        description="任务类型：'product'(全量), 'store'(店铺), 'repricing'(改价-最低价排行), 'listing_price'(上架-最低价)"
    )
    urls: List[str] = Field(
        ..., 
        description="链接列表"
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
    model_config = {"extra": "allow"}

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: str
    processed: int = 0
    total: int = 0
    created_at: str
    completed_at: Optional[str] = None
    results_count: int = 0
    results: Optional[List[Dict[str, Any]]] = None