from typing import Type

from sqlparse import sql
from sqlparse.tokens import Name


# find first token of the given type in the parsed query (using depth-first-search)
def find_first_instance(token: sql.Token, instancetype: Type) -> sql.Token:
    if not token.is_group:
        return None

    for subtoken in token.tokens:
        if isinstance(subtoken, instancetype):
            return subtoken
        elif subtoken.is_group:
            result = find_first_instance(subtoken, instancetype)
            if result is not None:
                return result

    return None


# find first token of the given ttype in the parsed query (using depth-first-search)
def find_first_ttype(token: sql.Token, ttype: str) -> sql.Token:
    if not token.is_group:
        return None

    for subtoken in token.tokens:
        if subtoken.ttype == ttype:
            return subtoken
        elif subtoken.is_group:
            result = find_first_ttype(subtoken, ttype)
            if result is not None:
                return result

    return None


# find last token of the given ttype in the parsed query (using depth-first-search on the reversed token list)
def find_last_ttype(token: sql.Token, ttype: str) -> sql.Token:
    if not token.is_group:
        return None

    for subtoken in reversed(token.tokens):
        if subtoken.ttype == ttype:
            return subtoken
        elif subtoken.is_group:
            result = find_last_ttype(subtoken, ttype)
            if result is not None:
                return result


# compare two tokens for equality
def compare_tokens(token1: sql.Token, token2: sql.Token) -> bool:
    # check if normalized string of both is identical
    if token1.normalized.lower() == token2.normalized.lower():
        return True

    # check if type is identical and both have the same attributes
    equal = isinstance(token1, type(token2)) and isinstance(token2, type(token2)) and (
            token1.is_group == token2.is_group) and (token1.is_keyword == token2.is_keyword) and (
                    token1.is_whitespace == token2.is_whitespace) and (token1.ttype == token2.ttype)

    if equal and (token1.is_keyword or token1.ttype == Name):
        # token is no group no need for recursion
        return equal and (token1.normalized.lower() == token2.normalized.lower())
    elif equal and token1.is_group:
        # remove whitespaces
        i, j = 0, 0
        while equal and i < len(token1.tokens) and j < len(token2.tokens):
            if token1.tokens[i].is_whitespace:
                i += 1
            elif token2.tokens[j].is_whitespace:
                j += 1
            else:
                # recursive call to compare childrens
                equal = equal and compare_tokens(token1.tokens[i], token2.tokens[j])
                i += 1
                j += 1

        return equal and i == len(token1.tokens) and j == len(token2.tokens)
    else:
        return equal
