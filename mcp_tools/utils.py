

def create_transition_data(zone_visits_list):
    """
    방문 구역 목록에서 전이 데이터를 생성합니다.
    
    Args:
        zone_visits_list: 리스트의 리스트 형태로, 각 내부 리스트는 한 고객의 방문 순서를 담고 있음
    
    Returns:
        전이 카운트를 담은 딕셔너리 리스트 (from_zone, to_zone, transition_count)
    """
    # 전이 카운트를 저장할 딕셔너리
    transition_counts = {}
    
    # 모든 고객의 방문 데이터를 처리
    for zone_visits in zone_visits_list:
        # 연속된 구역 간의 전이를 추출 (한 고객의 방문 내에서만)
        for i in range(len(zone_visits) - 1):
            from_zone = zone_visits[i]
            to_zone = zone_visits[i + 1]
            
            # 자기 자신과의 전이는 제외
            if from_zone == to_zone:
                continue
                
            # 키 생성 (from_zone -> to_zone)
            key = (from_zone, to_zone)
            
            # 카운트 증가
            if key in transition_counts:
                transition_counts[key] += 1
            else:
                transition_counts[key] = 1
    
    # 결과를 딕셔너리 리스트 형태로 변환
    result = []
    for (from_zone, to_zone), count in transition_counts.items():
        result.append({
            'from_zone': from_zone,
            'to_zone': to_zone,
            'transition_count': count
        })
    
    # 전이 횟수 기준으로 내림차순 정렬
    result.sort(key=lambda x: x['transition_count'], reverse=True)
    
    return result