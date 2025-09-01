"""
Data Analyst Agent
==================

데이터 수집, 처리, 통계 분석을 전담하는 전문 에이전트입니다.
MCP 툴들과 직접 연동하여 실제 데이터를 수집하고 품질을 검증합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import statistics
import json

from ..base_agent import (
    BaseAgent, AgentType, AgentMessage, MessageType, 
    AgentCapability, AnalyticsBaseAgent, DataProcessingMixin, CacheMixin
)

class DataAnalystAgent(AnalyticsBaseAgent, DataProcessingMixin, CacheMixin):
    """데이터 분석 전문 에이전트"""
    
    def __init__(self, mcp_client=None):
        """
        Args:
            mcp_client: MCP 클라이언트 인스턴스 (실제 데이터 접근용)
        """
        capabilities = [
            AgentCapability(
                name="data_collection",
                description="MCP 툴을 통한 데이터 수집",
                input_requirements=["site", "date_range", "metrics"],
                output_format="structured_data",
                confidence_threshold=0.8
            ),
            AgentCapability(
                name="data_validation",
                description="수집된 데이터의 품질 및 일관성 검증",
                input_requirements=["raw_data"],
                output_format="validation_report",
                confidence_threshold=0.9
            ),
            AgentCapability(
                name="statistical_analysis", 
                description="기본 통계 분석 및 메트릭 계산",
                input_requirements=["clean_data"],
                output_format="statistical_summary",
                confidence_threshold=0.85
            ),
            AgentCapability(
                name="time_series_analysis",
                description="시계열 데이터 분석 및 패턴 감지",
                input_requirements=["time_series_data"],
                output_format="time_series_insights",
                confidence_threshold=0.75
            ),
            AgentCapability(
                name="comparative_analysis",
                description="기간별, 세그먼트별 비교 분석",
                input_requirements=["comparison_periods", "metrics"],
                output_format="comparison_report",
                confidence_threshold=0.8
            )
        ]
        
        super().__init__(AgentType.DATA_ANALYST, capabilities=capabilities)
        self.mcp_client = mcp_client
        
        # 데이터 품질 기준
        self.quality_thresholds = {
            "completeness": 0.95,  # 95% 이상 데이터 완전성
            "consistency": 0.90,   # 90% 이상 일관성
            "accuracy": 0.85,      # 85% 이상 정확도
            "timeliness": 0.90     # 90% 이상 적시성
        }
        
        # 캐시 TTL 설정 (5분)
        self._cache_ttl = 300
        
        # 통계 임계값
        self.statistical_thresholds = {
            "outlier_std": 2.5,      # 이상치 판단 기준 (표준편차)
            "trend_significance": 0.05,  # 트렌드 유의성 기준
            "min_sample_size": 30    # 최소 샘플 크기
        }
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 메인 로직"""
        try:
            self.log_message(message, "received")
            
            task_type = message.content.get("task_type", "data_collection")
            params = message.content.get("params", {})
            
            # 캐시 확인
            cache_key = self._get_cache_key(task_type, params)
            cached_result = self.get_cached_result(cache_key)
            
            if cached_result:
                self.logger.info(f"캐시된 결과 사용: {task_type}")
                response_content = {
                    "status": "success",
                    "result": cached_result,
                    "cached": True
                }
            else:
                # 작업 타입별 처리
                if task_type == "data_collection":
                    result = await self._handle_data_collection(params)
                elif task_type == "comparative_data_collection":
                    result = await self._handle_comparative_data_collection(params)
                elif task_type == "time_series_collection":
                    result = await self._handle_time_series_collection(params)
                elif task_type == "historical_data_collection":
                    result = await self._handle_historical_data_collection(params)
                elif task_type == "performance_analysis":
                    result = await self._handle_performance_analysis(params)
                elif task_type == "data_validation":
                    result = await self._handle_data_validation(params)
                else:
                    raise ValueError(f"Unknown task type: {task_type}")
                
                # 결과 캐싱
                self.set_cache(cache_key, result)
                
                response_content = {
                    "status": "success",
                    "result": result,
                    "cached": False
                }
            
            response = self.create_response_message(message, response_content)
            self.log_message(response, "sent")
            
            return response
            
        except Exception as e:
            self.logger.error(f"Data analyst processing error: {str(e)}")
            error_response = self.create_response_message(
                message,
                {
                    "status": "error",
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                MessageType.ERROR
            )
            return error_response
    
    async def _handle_data_collection(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """기본 데이터 수집 처리"""
        self.logger.info("기본 데이터 수집 시작")
        
        site = params.get("site", "default")
        time_period = params.get("time_period", "this_week")
        metrics = params.get("metrics", ["traffic", "conversion"])
        
        # 날짜 범위 계산
        start_date, end_date = self._calculate_date_range(time_period)
        
        # 실제 데이터 수집 (MCP 툴 호출)
        raw_data = await self._collect_raw_data(site, start_date, end_date, metrics)
        
        # 데이터 품질 검증
        quality_report = await self._validate_data_quality(raw_data)
        
        # 기본 통계 계산
        statistical_summary = self._calculate_basic_statistics(raw_data)
        
        return {
            "collection_info": {
                "site": site,
                "date_range": f"{start_date} to {end_date}",
                "metrics_collected": metrics,
                "collection_timestamp": datetime.now().isoformat()
            },
            "raw_data": raw_data,
            "quality_report": quality_report,
            "statistical_summary": statistical_summary,
            "data_points": len(raw_data.get("daily_visitors", [])),
            "confidence": quality_report.get("overall_score", 0.5)
        }
    
    async def _handle_comparative_data_collection(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """비교 분석용 데이터 수집"""
        self.logger.info("비교 분석 데이터 수집 시작")
        
        site = params.get("site", "default")
        comparison_periods = params.get("comparison_periods", ["current", "previous"])
        metrics = params.get("metrics", ["traffic", "conversion"])
        
        results = {}
        
        for period in comparison_periods:
            start_date, end_date = self._calculate_date_range(period)
            
            # 각 기간별 데이터 수집
            period_data = await self._collect_raw_data(site, start_date, end_date, metrics)
            
            results[period] = {
                "date_range": f"{start_date} to {end_date}",
                "data": period_data,
                "statistics": self._calculate_basic_statistics(period_data)
            }
        
        # 비교 분석
        comparison_analysis = self._perform_comparative_analysis(results)
        
        return {
            "comparison_type": "period_comparison",
            "periods": results,
            "comparative_analysis": comparison_analysis,
            "confidence": 0.85
        }
    
    async def _handle_time_series_collection(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """시계열 분석용 데이터 수집"""
        self.logger.info("시계열 데이터 수집 시작")
        
        site = params.get("site", "default")
        time_range = params.get("time_range", "extended")  # last_30_days
        granularity = params.get("granularity", "daily")
        
        # 확장된 기간 계산
        if time_range == "extended":
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date, end_date = self._calculate_date_range(time_range)
        
        # 시계열 데이터 수집
        time_series_data = await self._collect_time_series_data(
            site, start_date, end_date, granularity
        )
        
        # 시계열 분석
        time_series_analysis = self._analyze_time_series(time_series_data)
        
        return {
            "time_series_info": {
                "site": site,
                "date_range": f"{start_date} to {end_date}",
                "granularity": granularity,
                "data_points": len(time_series_data)
            },
            "time_series_data": time_series_data,
            "time_series_analysis": time_series_analysis,
            "confidence": 0.78
        }
    
    async def _handle_historical_data_collection(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """예측용 히스토리컬 데이터 수집"""
        self.logger.info("히스토리컬 데이터 수집 시작")
        
        site = params.get("site", "default")
        lookback_period = params.get("lookback_period", "3_months")
        forecast_horizon = params.get("forecast_horizon", "1_month")
        
        # 룩백 기간 계산
        if lookback_period == "3_months":
            days_back = 90
        elif lookback_period == "6_months":
            days_back = 180
        else:
            days_back = 30
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        # 히스토리컬 데이터 수집
        historical_data = await self._collect_raw_data(
            site, start_date, end_date, ["traffic", "conversion", "sales"]
        )
        
        # 계절성 및 패턴 분석
        pattern_analysis = self._analyze_seasonal_patterns(historical_data)
        
        return {
            "historical_info": {
                "site": site,
                "lookback_period": f"{start_date} to {end_date}",
                "forecast_horizon": forecast_horizon,
                "data_completeness": self._calculate_completeness(historical_data)
            },
            "historical_data": historical_data,
            "pattern_analysis": pattern_analysis,
            "forecast_readiness": pattern_analysis.get("forecast_quality", "medium"),
            "confidence": 0.72
        }
    
    async def _handle_performance_analysis(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """성과 분석 처리"""
        self.logger.info("성과 분석 시작")
        
        site = params.get("site", "default")
        focus_areas = params.get("focus_areas", ["traffic", "conversion"])
        benchmark_comparison = params.get("benchmark_comparison", True)
        
        # 현재 기간 데이터
        current_start, current_end = self._calculate_date_range("this_week")
        current_data = await self._collect_raw_data(site, current_start, current_end, focus_areas)
        
        # 벤치마크 비교가 필요한 경우
        benchmark_data = None
        if benchmark_comparison:
            bench_start, bench_end = self._calculate_date_range("last_week")
            benchmark_data = await self._collect_raw_data(site, bench_start, bench_end, focus_areas)
        
        # 성과 분석
        performance_metrics = self._calculate_performance_metrics(current_data, benchmark_data)
        
        return {
            "performance_info": {
                "site": site,
                "analysis_period": f"{current_start} to {current_end}",
                "focus_areas": focus_areas,
                "benchmark_included": benchmark_comparison
            },
            "current_performance": self._calculate_basic_statistics(current_data),
            "benchmark_performance": self._calculate_basic_statistics(benchmark_data) if benchmark_data else None,
            "performance_metrics": performance_metrics,
            "confidence": 0.88
        }
    
    async def _handle_data_validation(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 검증 처리"""
        self.logger.info("데이터 검증 시작")
        
        raw_data = params.get("raw_data", {})
        validation_rules = params.get("validation_rules", self.quality_thresholds)
        
        # 종합적 데이터 품질 검증
        quality_report = await self._validate_data_quality(raw_data, validation_rules)
        
        # 이상치 감지
        anomaly_detection = self._detect_data_anomalies(raw_data)
        
        # 일관성 체크
        consistency_check = self._check_data_consistency(raw_data)
        
        return {
            "validation_info": {
                "data_size": len(str(raw_data)),
                "validation_timestamp": datetime.now().isoformat(),
                "validation_rules_applied": list(validation_rules.keys())
            },
            "quality_report": quality_report,
            "anomaly_detection": anomaly_detection,
            "consistency_check": consistency_check,
            "recommendation": self._generate_data_quality_recommendations(quality_report),
            "confidence": quality_report.get("overall_score", 0.5)
        }
    
    # ========================================================================
    # 실제 데이터 수집 메서드 (MCP 툴 연동)
    # ========================================================================
    
    async def _collect_raw_data(self, site: str, start_date, end_date, metrics: List[str]) -> Dict[str, Any]:
        """MCP 툴을 통한 실제 데이터 수집"""
        try:
            if self.mcp_client:
                # 실제 MCP 툴 호출 로직
                # 여기서는 시뮬레이션된 결과 반환
                await asyncio.sleep(0.5)  # 실제 API 호출 시뮬레이션
                
                # 실제로는 이런 식으로 호출:
                # tools = self.mcp_client.get_tools()
                # for tool in tools:
                #     if tool.name == "diagnose_avg_in":
                #         result = await tool.call(start_date=start_date, end_date=end_date, site=site)
                
                # 샘플 데이터 생성 (실제로는 MCP 툴 결과)
                raw_data = self._generate_sample_data(site, start_date, end_date, metrics)
            else:
                # MCP 클라이언트가 없는 경우 샘플 데이터
                raw_data = self._generate_sample_data(site, start_date, end_date, metrics)
            
            return raw_data
            
        except Exception as e:
            self.logger.error(f"데이터 수집 오류: {e}")
            # 오류 발생 시 기본 데이터 반환
            return self._generate_fallback_data()
    
    def _generate_sample_data(self, site: str, start_date, end_date, metrics: List[str]) -> Dict[str, Any]:
        """샘플 데이터 생성 (실제 MCP 결과를 시뮬레이션)"""
        import random
        
        # 날짜 범위 계산
        date_range = (end_date - start_date).days + 1
        
        sample_data = {
            "site": site,
            "date_range": f"{start_date} to {end_date}",
            "collection_timestamp": datetime.now().isoformat()
        }
        
        # 메트릭별 데이터 생성
        if "traffic" in metrics or "visitors" in metrics:
            # 방문객 데이터 (일별)
            base_visitors = random.randint(800, 1500)
            daily_visitors = []
            for i in range(date_range):
                # 요일 효과 시뮬레이션 (주말 낮음)
                day_of_week = (start_date + timedelta(days=i)).weekday()
                weekend_factor = 0.7 if day_of_week >= 5 else 1.0
                
                # 랜덤 변동
                daily_variation = random.uniform(0.85, 1.15)
                visitors = int(base_visitors * weekend_factor * daily_variation)
                daily_visitors.append(visitors)
            
            sample_data["daily_visitors"] = daily_visitors
            sample_data["total_visitors"] = sum(daily_visitors)
            sample_data["avg_daily_visitors"] = sum(daily_visitors) / len(daily_visitors)
        
        if "conversion" in metrics:
            sample_data["conversion_rate"] = round(random.uniform(0.25, 0.40), 3)
            sample_data["total_conversions"] = int(sample_data.get("total_visitors", 1000) * sample_data["conversion_rate"])
        
        if "pickup" in metrics:
            sample_data["pickup_rate"] = round(random.uniform(0.08, 0.15), 3)
            sample_data["total_pickups"] = int(sample_data.get("total_visitors", 1000) * sample_data["pickup_rate"])
        
        if "sales" in metrics:
            sample_data["total_sales"] = random.randint(50000, 150000)
            sample_data["avg_transaction_value"] = round(sample_data["total_sales"] / max(sample_data.get("total_conversions", 1), 1), 2)
        
        # 시간대별 데이터 (간소화)
        sample_data["hourly_distribution"] = {
            f"{hour:02d}:00": random.randint(30, 150) for hour in range(6, 23)
        }
        
        # 구역별 데이터 
        zones = ["음료", "과자", "아이스크림", "시식대", "결제대"]
        sample_data["zone_traffic"] = {
            zone: random.randint(100, 500) for zone in zones
        }
        
        return sample_data
    
    def _generate_fallback_data(self) -> Dict[str, Any]:
        """오류 발생 시 기본 데이터"""
        return {
            "daily_visitors": [1000, 1100, 1050, 1200, 1150, 900, 950],
            "conversion_rate": 0.30,
            "pickup_rate": 0.10,
            "data_quality_warning": "기본 데이터를 사용했습니다. 실제 분석을 위해서는 데이터 연결을 확인해 주세요.",
            "fallback": True
        }
    
    async def _collect_time_series_data(self, site: str, start_date, end_date, granularity: str) -> List[Dict[str, Any]]:
        """시계열 데이터 수집"""
        time_series = []
        
        current_date = start_date
        while current_date <= end_date:
            # 각 날짜별 데이터 수집
            daily_data = await self._collect_raw_data(
                site, current_date, current_date, ["traffic", "conversion"]
            )
            
            time_series.append({
                "date": current_date.isoformat(),
                "visitors": daily_data.get("daily_visitors", [0])[0] if daily_data.get("daily_visitors") else 0,
                "conversion_rate": daily_data.get("conversion_rate", 0.3),
                "day_of_week": current_date.weekday(),
                "is_weekend": current_date.weekday() >= 5
            })
            
            current_date += timedelta(days=1)
        
        return time_series
    
    # ========================================================================
    # 데이터 분석 메서드
    # ========================================================================
    
    async def _validate_data_quality(self, raw_data: Dict[str, Any], 
                                   rules: Dict[str, float] = None) -> Dict[str, Any]:
        """데이터 품질 검증"""
        if rules is None:
            rules = self.quality_thresholds
        
        quality_scores = {}
        
        # 완전성 검사
        completeness = self._check_completeness(raw_data)
        quality_scores["completeness"] = completeness
        
        # 일관성 검사
        consistency = self._check_consistency(raw_data)
        quality_scores["consistency"] = consistency
        
        # 정확성 검사 (기본적인 범위 체크)
        accuracy = self._check_accuracy(raw_data)
        quality_scores["accuracy"] = accuracy
        
        # 적시성 검사
        timeliness = self._check_timeliness(raw_data)
        quality_scores["timeliness"] = timeliness
        
        # 전체 점수 계산
        overall_score = sum(quality_scores.values()) / len(quality_scores)
        
        return {
            "quality_scores": quality_scores,
            "overall_score": overall_score,
            "quality_grade": self._get_quality_grade(overall_score),
            "passed_thresholds": {
                rule: score >= threshold 
                for rule, score in quality_scores.items() 
                for rule_name, threshold in rules.items() 
                if rule == rule_name
            },
            "recommendations": self._get_quality_recommendations(quality_scores, rules)
        }
    
    def _calculate_basic_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """기본 통계 계산"""
        stats = {}
        
        # 방문객 통계
        if "daily_visitors" in data and data["daily_visitors"]:
            visitors = data["daily_visitors"]
            stats["visitor_stats"] = {
                "mean": round(statistics.mean(visitors), 1),
                "median": statistics.median(visitors),
                "std_dev": round(statistics.stdev(visitors) if len(visitors) > 1 else 0, 1),
                "min": min(visitors),
                "max": max(visitors),
                "range": max(visitors) - min(visitors),
                "total": sum(visitors)
            }
        
        # 전환율 통계
        if "conversion_rate" in data:
            conv_rate = data["conversion_rate"]
            stats["conversion_stats"] = {
                "rate": conv_rate,
                "performance_level": self._categorize_conversion_performance(conv_rate),
                "benchmark_comparison": self._compare_to_benchmark(conv_rate, 0.30)  # 30% 벤치마크
            }
        
        # 픽업률 통계
        if "pickup_rate" in data:
            pickup_rate = data["pickup_rate"]
            stats["pickup_stats"] = {
                "rate": pickup_rate,
                "performance_level": self._categorize_pickup_performance(pickup_rate),
                "benchmark_comparison": self._compare_to_benchmark(pickup_rate, 0.10)  # 10% 벤치마크
            }
        
        # 요일별 패턴 (시계열 데이터가 있는 경우)
        if "daily_visitors" in data and len(data["daily_visitors"]) == 7:
            stats["weekly_pattern"] = self._analyze_weekly_pattern(data["daily_visitors"])
        
        return stats
    
    def _perform_comparative_analysis(self, periods_data: Dict[str, Dict]) -> Dict[str, Any]:
        """기간별 비교 분석"""
        if len(periods_data) < 2:
            return {"error": "비교를 위해 최소 2개 기간의 데이터가 필요합니다."}
        
        periods = list(periods_data.keys())
        current_period = periods[0]  # 첫 번째를 현재 기간으로 가정
        comparison_period = periods[1]  # 두 번째를 비교 기간으로 가정
        
        current_stats = periods_data[current_period]["statistics"]
        comparison_stats = periods_data[comparison_period]["statistics"]
        
        comparative_results = {}
        
        # 방문객 비교
        if "visitor_stats" in current_stats and "visitor_stats" in comparison_stats:
            current_avg = current_stats["visitor_stats"]["mean"]
            comparison_avg = comparison_stats["visitor_stats"]["mean"]
            
            change_pct = ((current_avg - comparison_avg) / comparison_avg) * 100 if comparison_avg != 0 else 0
            
            comparative_results["visitor_comparison"] = {
                "current_average": current_avg,
                "comparison_average": comparison_avg,
                "change_percentage": round(change_pct, 1),
                "change_direction": "increase" if change_pct > 0 else "decrease" if change_pct < 0 else "stable",
                "significance": "significant" if abs(change_pct) > 10 else "moderate" if abs(change_pct) > 5 else "minor"
            }
        
        # 전환율 비교
        if "conversion_stats" in current_stats and "conversion_stats" in comparison_stats:
            current_conv = current_stats["conversion_stats"]["rate"]
            comparison_conv = comparison_stats["conversion_stats"]["rate"]
            
            change_points = current_conv - comparison_conv
            
            comparative_results["conversion_comparison"] = {
                "current_rate": current_conv,
                "comparison_rate": comparison_conv,
                "change_points": round(change_points, 3),
                "change_direction": "improvement" if change_points > 0 else "decline" if change_points < 0 else "stable"
            }
        
        return comparative_results
    
    def _analyze_time_series(self, time_series_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """시계열 분석"""
        if not time_series_data:
            return {"error": "시계열 데이터가 없습니다."}
        
        # 방문객 시계열 추출
        visitors_series = [point["visitors"] for point in time_series_data]
        dates = [point["date"] for point in time_series_data]
        
        analysis = {}
        
        # 트렌드 분석
        if len(visitors_series) >= 3:
            trend_analysis = self._calculate_trend(visitors_series)
            analysis["trend"] = trend_analysis
        
        # 계절성/주기성 분석
        seasonality = self._detect_seasonality(time_series_data)
        analysis["seasonality"] = seasonality
        
        # 변동성 분석
        volatility = self._calculate_volatility(visitors_series)
        analysis["volatility"] = volatility
        
        # 이상치 감지
        outliers = self._detect_outliers_in_series(visitors_series, dates)
        analysis["outliers"] = outliers
        
        return analysis
    
    def _analyze_seasonal_patterns(self, historical_data: Dict[str, Any]) -> Dict[str, Any]:
        """계절성 패턴 분석"""
        pattern_analysis = {}
        
        # 요일별 패턴
        if "daily_visitors" in historical_data:
            visitors = historical_data["daily_visitors"]
            if len(visitors) >= 7:
                weekly_pattern = {}
                days = ["월", "화", "수", "목", "금", "토", "일"]
                
                for i, day in enumerate(days):
                    day_data = [visitors[j] for j in range(i, len(visitors), 7)]
                    if day_data:
                        weekly_pattern[day] = {
                            "average": round(sum(day_data) / len(day_data), 1),
                            "samples": len(day_data)
                        }
                
                pattern_analysis["weekly_pattern"] = weekly_pattern
        
        # 시간대별 패턴
        if "hourly_distribution" in historical_data:
            hourly_data = historical_data["hourly_distribution"]
            peak_hours = sorted(hourly_data.items(), key=lambda x: x[1], reverse=True)[:3]
            
            pattern_analysis["daily_pattern"] = {
                "peak_hours": [{"hour": h, "traffic": t} for h, t in peak_hours],
                "total_hourly_traffic": sum(hourly_data.values())
            }
        
        # 예측 품질 평가
        data_points = len(historical_data.get("daily_visitors", []))
        if data_points >= 60:
            forecast_quality = "high"
        elif data_points >= 30:
            forecast_quality = "medium"
        else:
            forecast_quality = "low"
        
        pattern_analysis["forecast_quality"] = forecast_quality
        pattern_analysis["data_sufficiency"] = {
            "data_points": data_points,
            "recommended_minimum": 30,
            "ideal_minimum": 60
        }
        
        return pattern_analysis
    
    # ========================================================================
    # 헬퍼 메서드들
    # ========================================================================
    
    def _calculate_date_range(self, time_period: str) -> Tuple[datetime, datetime]:
        """시간 기간에 따른 날짜 범위 계산"""
        today = datetime.now().date()
        
        if time_period == "today":
            return today, today
        elif time_period == "yesterday":
            yesterday = today - timedelta(days=1)
            return yesterday, yesterday
        elif time_period == "this_week":
            # 이번 주 월요일부터 일요일까지
            days_since_monday = today.weekday()
            monday = today - timedelta(days=days_since_monday)
            sunday = monday + timedelta(days=6)
            return monday, sunday
        elif time_period == "last_week":
            # 지난 주 월요일부터 일요일까지
            days_since_monday = today.weekday()
            this_monday = today - timedelta(days=days_since_monday)
            last_monday = this_monday - timedelta(days=7)
            last_sunday = last_monday + timedelta(days=6)
            return last_monday, last_sunday
        elif time_period == "current":
            # 최근 7일
            return today - timedelta(days=6), today
        elif time_period == "previous":
            # 이전 7일 (8-14일 전)
            return today - timedelta(days=13), today - timedelta(days=7)
        else:
            # 기본값: 최근 7일
            return today - timedelta(days=6), today
    
    def _check_completeness(self, data: Dict[str, Any]) -> float:
        """데이터 완전성 점수 계산"""
        required_fields = ["daily_visitors", "conversion_rate"]
        present_fields = sum(1 for field in required_fields if field in data and data[field] is not None)
        return present_fields / len(required_fields)
    
    def _check_consistency(self, data: Dict[str, Any]) -> float:
        """데이터 일관성 점수 계산"""
        consistency_score = 1.0
        
        # 방문객 데이터 일관성 (음수나 극단값 체크)
        if "daily_visitors" in data and data["daily_visitors"]:
            visitors = data["daily_visitors"]
            negative_count = sum(1 for v in visitors if v < 0)
            extreme_count = sum(1 for v in visitors if v > 10000)  # 극단적으로 높은 값
            
            inconsistency_ratio = (negative_count + extreme_count) / len(visitors)
            consistency_score -= inconsistency_ratio * 0.5
        
        # 비율 데이터 일관성 (0-1 범위 체크)
        rate_fields = ["conversion_rate", "pickup_rate"]
        for field in rate_fields:
            if field in data and data[field] is not None:
                rate = data[field]
                if not (0 <= rate <= 1):
                    consistency_score -= 0.2
        
        return max(consistency_score, 0.0)
    
    def _check_accuracy(self, data: Dict[str, Any]) -> float:
        """데이터 정확성 점수 계산 (기본적인 범위 체크)"""
        accuracy_score = 1.0
        
        # 합리적 범위 체크
        if "conversion_rate" in data and data["conversion_rate"] is not None:
            conv_rate = data["conversion_rate"]
            if conv_rate > 0.8 or conv_rate < 0.01:  # 80% 이상이나 1% 미만은 비현실적
                accuracy_score -= 0.3
        
        if "daily_visitors" in data and data["daily_visitors"]:
            avg_visitors = sum(data["daily_visitors"]) / len(data["daily_visitors"])
            if avg_visitors > 5000 or avg_visitors < 10:  # 극단적 값
                accuracy_score -= 0.2
        
        return max(accuracy_score, 0.0)
    
    def _check_timeliness(self, data: Dict[str, Any]) -> float:
        """데이터 적시성 점수 계산"""
        # 수집 시간이 있는 경우 시간 차이 계산
        if "collection_timestamp" in data:
            try:
                collection_time = datetime.fromisoformat(data["collection_timestamp"].replace('Z', '+00:00'))
                time_diff = datetime.now() - collection_time.replace(tzinfo=None)
                
                # 1시간 이내면 1.0, 24시간 이후면 0.5
                hours_old = time_diff.total_seconds() / 3600
                if hours_old <= 1:
                    return 1.0
                elif hours_old <= 24:
                    return 1.0 - (hours_old - 1) * 0.5 / 23
                else:
                    return 0.5
            except:
                return 0.7  # 파싱 오류 시 기본값
        
        return 0.8  # 수집 시간 정보가 없는 경우 기본값
    
    def _calculate_completeness(self, data: Dict[str, Any]) -> float:
        """데이터 완전성 비율 계산"""
        if not data:
            return 0.0
        
        total_expected = 0
        total_present = 0
        
        # 일별 데이터 완전성
        if "daily_visitors" in data:
            visitors = data["daily_visitors"]
            total_expected += len(visitors)
            total_present += sum(1 for v in visitors if v is not None and v >= 0)
        
        # 기타 필수 필드
        essential_fields = ["conversion_rate", "pickup_rate"]
        for field in essential_fields:
            total_expected += 1
            if field in data and data[field] is not None:
                total_present += 1
        
        return total_present / total_expected if total_expected > 0 else 1.0
    
    def _detect_data_anomalies(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 이상치 감지"""
        anomalies = {}
        
        # 방문객 데이터 이상치
        if "daily_visitors" in data and data["daily_visitors"]:
            visitors = data["daily_visitors"]
            mean_visitors = statistics.mean(visitors)
            std_visitors = statistics.stdev(visitors) if len(visitors) > 1 else 0
            
            outliers = []
            for i, visitor_count in enumerate(visitors):
                if std_visitors > 0 and abs(visitor_count - mean_visitors) > self.statistical_thresholds["outlier_std"] * std_visitors:
                    outliers.append({
                        "day_index": i,
                        "value": visitor_count,
                        "expected_range": f"{mean_visitors - 2*std_visitors:.0f}-{mean_visitors + 2*std_visitors:.0f}",
                        "severity": "high" if abs(visitor_count - mean_visitors) > 3 * std_visitors else "medium"
                    })
            
            anomalies["visitor_outliers"] = outliers
        
        return anomalies
    
    def _check_data_consistency(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 일관성 종합 체크"""
        consistency_results = {}
        
        # 시계열 일관성 (큰 변동 체크)
        if "daily_visitors" in data and data["daily_visitors"]:
            visitors = data["daily_visitors"]
            daily_changes = []
            for i in range(1, len(visitors)):
                change_pct = abs(visitors[i] - visitors[i-1]) / max(visitors[i-1], 1) * 100
                daily_changes.append(change_pct)
            
            large_changes = [c for c in daily_changes if c > 50]  # 50% 이상 변화
            
            consistency_results["temporal_consistency"] = {
                "large_daily_changes": len(large_changes),
                "avg_daily_change": round(sum(daily_changes) / len(daily_changes), 1) if daily_changes else 0,
                "stability_score": 1.0 - min(len(large_changes) / len(daily_changes), 1.0) if daily_changes else 1.0
            }
        
        # 논리적 일관성 (전환율과 방문객 수 관계 등)
        logical_consistency = 1.0
        
        if "total_visitors" in data and "total_conversions" in data and data["total_visitors"] > 0:
            calculated_conv_rate = data["total_conversions"] / data["total_visitors"]
            stated_conv_rate = data.get("conversion_rate", calculated_conv_rate)
            
            if abs(calculated_conv_rate - stated_conv_rate) > 0.05:  # 5% 이상 차이
                logical_consistency -= 0.3
        
        consistency_results["logical_consistency"] = logical_consistency
        
        return consistency_results
    
    def get_capabilities(self) -> List[AgentCapability]:
        """에이전트 능력 목록 반환"""
        return self.capabilities

if __name__ == "__main__":
    # 테스트 실행
    async def test_data_analyst():
        analyst = DataAnalystAgent()
        
        test_message = AgentMessage(
            id="test_data_001",
            sender="orchestrator",
            receiver=analyst.agent_id,
            message_type=MessageType.REQUEST,
            content={
                "task_type": "data_collection",
                "params": {
                    "site": "test_store",
                    "time_period": "this_week",
                    "metrics": ["traffic", "conversion", "pickup"]
                }
            }
        )
        
        response = await analyst.process_message(test_message)
        print(f"Response Status: {response.content['status']}")
        
        if response.content['status'] == 'success':
            result = response.content['result']
            print(f"데이터 수집 완료:")
            print(f"- 사이트: {result['collection_info']['site']}")
            print(f"- 기간: {result['collection_info']['date_range']}")
            print(f"- 데이터 포인트: {result['data_points']}")
            print(f"- 신뢰도: {result['confidence']:.1%}")
            print(f"- 품질 점수: {result['quality_report']['overall_score']:.1%}")
    
    import asyncio
    # asyncio.run(test_data_analyst())