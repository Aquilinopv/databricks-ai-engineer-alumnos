"""Adapter for managed Llama Guard safety filters; never treats an API failure as safe."""
import json

def gateway_verdict(invoke, text):
    # The gateway, not the generative Llama Instruct model, judges the submitted text.
    try:
        # Submit the text unchanged: an added benign system instruction changed
        # the observed safety verdict in the first remote integration test.
        response = invoke({'messages': [{'role': 'user', 'content': text}],
                           'max_tokens': 4, 'temperature': 0})
    except Exception as exc:
        try:
            error = json.loads(str(exc))
        except (ValueError, TypeError):
            raise RuntimeError('gateway_unavailable') from exc
        # Only the explicit guardrail error is a safety rejection; auth/timeouts remain errors.
        if error.get('finishReason') in {'input_guardrail_triggered', 'output_guardrail_triggered'}:
            field = 'input_guardrail' if error['finishReason'].startswith('input') else 'output_guardrail'
            if any(item.get('flagged') is True for item in error.get(field, [])):
                return 'unsafe'
        raise RuntimeError('gateway_unavailable') from exc
    choices = response.get('choices') if isinstance(response, dict) else None
    if not isinstance(choices, list) or not choices:
        raise RuntimeError('gateway_invalid_response')
    for choice in choices:
        if not isinstance(choice, dict) or choice.get('finish_reason') not in {'stop', 'length'}:
            raise RuntimeError('gateway_invalid_response')
        message = choice.get('message')
        if not isinstance(message, dict) or message.get('role') != 'assistant' or not isinstance(message.get('content'), str) or not message['content'].strip():
            raise RuntimeError('gateway_invalid_response')
    return 'safe'
