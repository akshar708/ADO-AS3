#include <string.h>
#include <stdlib.h>

#include "dberror.h"
#include "record_mgr.h"
#include "expr.h"
#include "tables.h"

// implementations

RC valueSmaller(Value *left, Value *right, Value *result) {
    // Check if the data types are the same
    if (left->dt != right->dt) {
        return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE; // Return error code for different data types
    }

    // Set the result data type to boolean
    result->dt = DT_BOOL;

    // Use a for loop to handle the comparison
    for (int i = 0; i < 1; i++) {  // The loop will run exactly once
        switch (left->dt) {
            case DT_INT:
                result->v.boolV = (left->v.intV < right->v.intV);
                break;
            case DT_FLOAT:
                result->v.boolV = (left->v.floatV < right->v.floatV);
                break;
            case DT_BOOL:
                result->v.boolV = (left->v.boolV < right->v.boolV);
                break; // Added break to prevent fall-through
            case DT_STRING:
                result->v.boolV = (strcmp(left->v.stringV, right->v.stringV) < 0);
                break;
            default:
                return RC_UNSUPPORTED_DATATYPE; // Handle unsupported data types
        }
    }

    return RC_OK; // Return success
}

RC boolNot(Value *input, Value *result) {
    // Check if the input data type is boolean
    if (input->dt != DT_BOOL) {
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean NOT requires boolean input");
    }

    // Set the result data type to boolean
    result->dt = DT_BOOL;

    // Use a for loop to perform the NOT operation
    for (int i = 0; i < 1; i++) {  // The loop will run exactly once
        result->v.boolV = !(input->v.boolV);
    }

    return RC_OK; // Return success
}

RC
boolOr (Value *left, Value *right, Value *result)
{
	if (left->dt != DT_BOOL || right->dt != DT_BOOL)
		THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean OR requires boolean inputs");
	result->v.boolV = (left->v.boolV || right->v.boolV);

	return RC_OK;
}

RC evalExpr(Record *record, Schema *schema, Expr *expr, Value **result) {
    Value *lIn = NULL;
    Value *rIn = NULL;
    MAKE_VALUE(*result, DT_INT, -1);
    
    // Use a do-while loop to handle the evaluation of the expression
    do {
        switch (expr->type) {
            case EXPR_OP: {
                Operator *op = expr->expr.op;
                bool twoArgs = (op->type != OP_BOOL_NOT);

                CHECK(evalExpr(record, schema, op->args[0], &lIn));
                if (twoArgs) {
                    CHECK(evalExpr(record, schema, op->args[1], &rIn));
                }

                switch (op->type) {
                    case OP_BOOL_NOT:
                        CHECK(boolNot(lIn, *result));
                        break;
                    case OP_BOOL_AND:
                        CHECK(boolAnd(lIn, rIn, *result));
                        break;
                    case OP_BOOL_OR:
                        CHECK(boolOr(lIn, rIn, *result));
                        break;
                    case OP_COMP_EQUAL:
                        CHECK(valueEquals(lIn, rIn, *result));
                        break;
                    case OP_COMP_SMALLER:
                        CHECK(valueSmaller(lIn, rIn, *result));
                        break;
                    default:
                        break;
                }

                // Cleanup
                freeVal(lIn);
                if (twoArgs) {
                    freeVal(rIn);
                }
                break; // Exit the switch for EXPR_OP
            }
            case EXPR_CONST:
                CPVAL(*result, expr->expr.cons);
                break;
            case EXPR_ATTRREF:
                free(*result);
                CHECK(getAttr(record, schema, expr->expr.attrRef, result));
                break;
        }
    } while (0); // The loop runs only once

    return RC_OK;
}
RC
freeExpr (Expr *expr)
{
	switch(expr->type)
	{
	case EXPR_OP:
	{
		Operator *op = expr->expr.op;
		switch(op->type)
		{
		case OP_BOOL_NOT:
			freeExpr(op->args[0]);
			break;
		default:
			freeExpr(op->args[0]);
			freeExpr(op->args[1]);
			break;
		}
		free(op->args);
	}
	break;
	case EXPR_CONST:
		freeVal(expr->expr.cons);
		break;
	case EXPR_ATTRREF:
		break;
	}
	free(expr);

	return RC_OK;
}

void 
freeVal (Value *val)
{
	if (val->dt == DT_STRING)
		free(val->v.stringV);
	free(val);
}

