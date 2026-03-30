from fastapi import FastAPI
from routers import metrics



app = FastAPI()
app.include_router(metrics.router)


@app.get("/")
def root():
    return {"status":"ok "}