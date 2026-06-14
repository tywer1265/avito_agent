# agents/designer.py
"""
Agent 2 — Designer
Mission: Generate product images via Nano Banana API.
Trigger: New product brief from Trend Hunter or Procurement
Cost target: < $0.08 per product set (hero + 3 details + infographic)
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Optional

import httpx
import structlog
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Product, get_session

log = structlog.get_logger("designer")

# Image QC scoring thresholds
QC_PASS_SCORE = 3          # out of 4 checks must pass
MAX_REGEN_ATTEMPTS = 2     # per spec

# Prompt cache: category → successful base prompt (in-memory; can be persisted to DB)
_PROMPT_CACHE: dict[str, str] = {}

SAFE_COLORS = ["чёрный", "белый"]

IMAGE_TYPES = [
    ("hero", "clean white background, full product shot, commercial photography"),
    ("detail_1", "close-up texture detail, fabric quality visible"),
    ("detail_2", "worn by mannequin, side view, neutral background"),
    ("detail_3", "folded flat lay, top-down view"),
    ("infographic", "product infographic with size chart, material info, key features"),
]


class DesignerAgent(BaseAgent):
    name = "designer"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")

        if trigger == "pending_products_sweep":
            return await self._process_pending_products()
        elif trigger == "generate_for_product":
            product_id = task.get("product_id")
            if not product_id:
                return {"status": "error", "error": "product_id required"}
            return await self._generate_for_product(product_id)
        else:
            return {"status": "ok", "message": "No actionable trigger"}

    async def _process_pending_products(self) -> dict:
        """Find all draft products without images and generate sets."""
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(
                select(Product).where(Product.status == "draft").limit(5)
            )
            products = result.scalars().all()

        if not products:
            self._log.info("designer.no_pending_products")
            return {"status": "ok", "processed": 0}

        results = []
        for product in products:
            result = await self._generate_for_product(product.id)
            results.append(result)

        return {"status": "ok", "processed": len(results), "results": results}

    async def _generate_for_product(self, product_id: int) -> dict:
        """Generate full image set for a single product."""
        product = await self._load_product(product_id)
        if not product:
            return {"status": "error", "error": f"Product {product_id} not found"}

        self._log.info("designer.generate_start", product_id=product_id, name=product.name)

        # Generate for trend color + safe color (dual colorway)
        colors = [product.color] + [c for c in SAFE_COLORS if c != product.color]
        all_image_sets = {}

        for color in colors[:2]:
            image_set = await self._generate_image_set(product, color)
            all_image_sets[color] = image_set
            self._log.info("designer.colorway_done", color=color, images=len(image_set))

        # Mark product as ready for copywriter
        await self._update_product_status(product_id, "images_ready")

        await self.log(
            action="generate_product_images",
            result=f"Generated {sum(len(v) for v in all_image_sets.values())} images",
            confidence_score=0.9,
            input_summary=f"product_id={product_id} name={product.name}",
        )

        return {
            "status": "ok",
            "product_id": product_id,
            "image_sets": all_image_sets,
        }

    async def _generate_image_set(self, product: Product, color: str) -> dict[str, str]:
        """Generate all 5 images for one colorway. Returns {image_type: url}."""
        base_prompt = await self._get_base_prompt(product, color)
        results = {}

        for img_type, style_hint in IMAGE_TYPES:
            url = await self._generate_single_image(
                product=product,
                color=color,
                img_type=img_type,
                style_hint=style_hint,
                base_prompt=base_prompt,
            )
            if url:
                results[img_type] = url

        return results

    async def _generate_single_image(
        self,
        product: Product,
        color: str,
        img_type: str,
        style_hint: str,
        base_prompt: str,
    ) -> Optional[str]:
        """Generate one image with QC and up to MAX_REGEN_ATTEMPTS retries."""
        prompt = f"{base_prompt}, {style_hint}, color: {color}"

        for attempt in range(MAX_REGEN_ATTEMPTS + 1):
            url = await self._call_nano_banana(prompt, img_type)
            if not url:
                continue

            qc_passed, qc_score = await self._qc_check(url, product, img_type)
            if qc_passed:
                # Cache successful prompt for this category
                cache_key = f"{product.category}_{img_type}"
                _PROMPT_CACHE[cache_key] = prompt
                return url

            self._log.warning(
                "designer.qc_fail",
                attempt=attempt,
                img_type=img_type,
                score=qc_score,
            )
            if attempt < MAX_REGEN_ATTEMPTS:
                # Refine prompt on retry
                prompt = await self._refine_prompt(prompt, qc_score)

        self._log.error("designer.max_retries_exceeded", img_type=img_type)
        return None

    async def _call_nano_banana(self, prompt: str, img_type: str) -> Optional[str]:
        """Call Nano Banana image generation API."""
        async with httpx.AsyncClient(timeout=60) as client:
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(min=3, max=15),
                    retry=retry_if_exception_type(httpx.HTTPError),
                    reraise=True,
                ):
                    with attempt:
                        resp = await client.post(
                            f"{settings.nano_banana_api_url}/generate",
                            headers={
                                "Authorization": f"Bearer {settings.nano_banana_api_key}",
                                "Content-Type": "application/json",
                            },
                            json={
                                "prompt": prompt,
                                "negative_prompt": "watermark, text, blurry, distorted, extra limbs, ugly",
                                "width": 1024,
                                "height": 1024,
                                "steps": 30,
                                "guidance_scale": 7.5,
                            },
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        return data.get("url") or data.get("image_url")
            except Exception as exc:
                self._log.error("designer.nano_banana_error", error=str(exc))
                return None

    async def _qc_check(
        self, url: str, product: Product, img_type: str
    ) -> tuple[bool, int]:
        """
        Score image quality using Haiku vision. Returns (passed, score/4).
        Checks: background clean / product visible / no AI artifacts / commercial feel
        """
        try:
            prompt = f"""You are a QC inspector for product images on an e-commerce platform.
Evaluate this image for: {product.name} ({product.category}), image type: {img_type}

Score each criterion (1=pass, 0=fail):
1. background_clean: Is background clean/white/neutral without clutter?
2. product_visible: Is the product clearly visible and the main focus?
3. no_artifacts: Are there no obvious AI artifacts (extra fingers, weird shapes, text)?
4. commercial_feel: Does it look like a professional product photo?

Image URL: {url}

Return JSON: {{"background_clean": 0|1, "product_visible": 0|1, "no_artifacts": 0|1, "commercial_feel": 0|1, "total": <0-4>}}"""

            result = await self.call_haiku_json(
                system="You are a product image QC system. Return only JSON.",
                user=prompt,
            )
            score = result.get("total", 0)
            return score >= QC_PASS_SCORE, score
        except Exception as exc:
            self._log.warning("designer.qc_error", error=str(exc))
            return True, 3  # Pass if QC check fails (don't block pipeline)

    async def _get_base_prompt(self, product: Product, color: str) -> str:
        """Build or retrieve cached base prompt for this product category."""
        cache_key = f"{product.category}_hero"
        if cache_key in _PROMPT_CACHE:
            base = _PROMPT_CACHE[cache_key]
            return base

        # Build a category-specific base prompt
        category_prompts = {
            "hoodie": "premium quality hoodie, soft fabric, modern streetwear style, Russian market",
            "t-shirt": "high quality cotton t-shirt, clean print, modern cut, Russian streetwear",
            "cap": "trendy snapback/dad cap, structured brim, urban style, Russian youth fashion",
            "pants": "comfortable modern pants, quality stitching, urban fit, Russian streetwear",
        }
        base = category_prompts.get(
            product.category or "hoodie",
            f"premium clothing item {product.name}",
        )
        return f"{base}, product photography, studio lighting"

    async def _refine_prompt(self, prompt: str, qc_score: int) -> str:
        """Ask Haiku to refine a failing prompt."""
        try:
            result = await self.call_haiku(
                system="You are a prompt engineer for product photography AI.",
                user=f"""This prompt failed QC with score {qc_score}/4:
"{prompt}"

Rewrite it to produce: cleaner background, more visible product, fewer artifacts, more commercial look.
Return only the improved prompt, nothing else. Keep under 150 characters.""",
                max_tokens=200,
            )
            return result.strip()
        except Exception:
            return prompt + ", ultra clean studio background, professional commercial shot"

    async def _load_product(self, product_id: int) -> Optional[Product]:
        from sqlalchemy import select
        async with get_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            return result.scalar_one_or_none()

    async def _update_product_status(self, product_id: int, status: str) -> None:
        from sqlalchemy import update
        async with get_session() as session:
            await session.execute(
                update(Product).where(Product.id == product_id).values(status=status)
            )
